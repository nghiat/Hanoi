use clap::{Parser, ValueEnum};
use std::io::{self, BufRead, BufReader, ErrorKind};
use std::fs::{self, DirEntry};
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::time::Instant;
use interprocess::local_socket::{LocalSocketListener, LocalSocketStream};
use std::io::Read;
use std::io::Write;
use std::sync::{Arc, Mutex};
use notify::{
    event::{self, EventKind},
    Watcher, RecommendedWatcher, RecursiveMode, Result
};
use regex::Regex;

#[derive(ValueEnum, Clone)]
enum OperatingMode {
    Server,
    Client,
}

enum Filter {
    Include(Regex),
    Exclude(Regex),
}

#[derive(Parser)]
struct Args {
    #[clap(value_enum, default_value_t = OperatingMode::Client)]
    #[arg(long)]
    mode: OperatingMode,

    #[arg(long)]
    root: Option<String>,

    term: Option<String>,
}

fn filter_file(configs: &Vec<Filter>, path: &Path, root: &Path) -> bool {
    let mut result = false;
    if let Ok(rel_path) = path.strip_prefix(root) {
        for filter in configs {
            match(filter) {
                Filter::Include(re) => {
                    if re.is_match(rel_path.display().to_string().as_str()) {
                        result = true;
                    }
                }
                Filter::Exclude(re) => {
                    if re.is_match(rel_path.display().to_string().as_str()) {
                        result = false;
                    }
                }
            }
        }
    }
    result
}

fn visit_dirs(dir: &Path, cb: &mut impl FnMut(&DirEntry)) -> io::Result<()> {
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_dirs(&path, cb)?;
            } else {
                cb(&entry);
            }
        }
    }
    Ok(())
}

struct ScopeTime {
    start: Instant,
}

impl Default for ScopeTime {
    fn default() -> ScopeTime {
        ScopeTime {
            start: Instant::now(),
        }
    }
}

impl Drop for ScopeTime {
    fn drop(&mut self) {
        let end_time = Instant::now();
        let elapsed_time = end_time.duration_since(self.start);
        let elapsed_ms = elapsed_time.as_secs() * 1000 + elapsed_time.subsec_millis() as u64;
        println!("{} ms", elapsed_ms);
    }
}

#[derive(Default)]
struct Indexer2 {
    root: PathBuf,
    files: HashMap<PathBuf, String>,
    filters: Vec<Filter>,
}

impl Indexer2 {
    const ENDING_MSG: &str = "###end###";
}

impl Indexer2 {
    fn build(&mut self, path: &Path) {
        self.root = PathBuf::from(path);
        let config_path = Path::new(path).join(".hanoi");
        self.filters = Vec::new();
        if let Ok(config_str) = std::fs::read_to_string(config_path) {
            for line in config_str.lines() {
                if line.trim().starts_with("#") {
                    // Ignore comment
                    continue;
                }
                if line.trim().starts_with("!") {
                    self.filters.push(Filter::Exclude(Regex::new(&line.trim()[1..]).unwrap()));
                } else {
                    self.filters.push(Filter::Include(Regex::new(&line.trim()).unwrap()));
                }
            }
        }

        let mut load_files = |dir_entry: &DirEntry| {
            if !filter_file(&self.filters, dir_entry.path().as_path(), &path) {
                return;
            }
            let path_buf = dir_entry.path();
            if let Ok(file_str) = std::fs::read_to_string(path_buf.as_path()) {
                self.files.insert(path_buf, file_str);
            }
        };
        let _ = visit_dirs(&path, &mut load_files);
        println!("Indexer2: Done building");
    }

    fn find(&self, term: &str, reader: &mut BufReader<LocalSocketStream>) {
        for (key, value) in &self.files {
            if let Some(pos) = value.find(&term) {
                let mut line_num = 1;
                for line in value.lines() {
                    if let Some(pos) = line.find(&term) {
                        reader.get_mut().write_all(format!("{}:{}: {}", key.display().to_string(), line_num, line).as_bytes());
                        reader.get_mut().write(b"\n");
                    }
                    line_num += 1;
                }
            }
        }
    }

    fn handle_event(&mut self, event: &event::Event) {
        match event.kind {
            EventKind::Create(_) | EventKind::Modify(_) => {
                for path in &event.paths {
                    if filter_file(&self.filters, path, self.root.as_path()) && path.is_file() {
                        if let Ok(file_str) = std::fs::read_to_string(path.as_path()) {
                            self.files.insert(PathBuf::clone(path), file_str);
                        }
                    }
                }
            },
            EventKind::Remove(remove) => {
                for path in &event.paths {
                    if filter_file(&self.filters, path, self.root.as_path()) && path.is_file() {
                        self.files.remove(path);
                    }
                }
            },
            _ => {}
        }
    }
}

fn find_existing_pipe_name(path: &Path) -> Option<PathBuf> {
    let mut named_pipe_path = path;
    loop {
        if LocalSocketListener::bind(named_pipe_path).is_err_and(|x| x.kind() == ErrorKind::PermissionDenied) {
            return Some(named_pipe_path.to_path_buf());
        }
        let parent_path = named_pipe_path.parent();
        match parent_path {
            Some(v) => named_pipe_path = v,
            None => break,
        }
    }
    None
}

fn server_main(args: &Args) {
    let root_str = args.root.as_ref().unwrap();
    let path = Path::new(root_str.as_str());
    let existing_pipe_name = find_existing_pipe_name(&path);
    if let Some(existing_pipe_name) = find_existing_pipe_name(&path) {
        println!("This directory or its parent directory has been indexed: {}", existing_pipe_name.display());
        return;
    }

    println!("Start indexing: {}", path.display());
    let named_pipe = LocalSocketListener::bind(path).unwrap();

    let mut indexer2 = Indexer2::default();
    {
        let scope_time = ScopeTime::default();
        indexer2.build(&path);
    }
    let indexer2 = Arc::new(Mutex::new(indexer2));
    let mut watcher;
    {
        let indexer2 = indexer2.clone();
        watcher = notify::recommended_watcher(move |res: Result<event::Event>| {
            match res {
               Ok(event) => indexer2.lock().unwrap().handle_event(&event),
               Err(e) => println!("watch error: {:?}", e),
            }
        }).unwrap();
    }
    watcher.watch(&path, RecursiveMode::Recursive);

    loop {
        for incoming in named_pipe.incoming() {
            if let Some(stream) = incoming.ok() {
                let mut incoming_reader = BufReader::new(stream);
                let mut buffer = String::with_capacity(128);
                incoming_reader.read_line(&mut buffer);
                let client_args = Args::parse_from(buffer.trim().split(" "));
                if let Some(term) = client_args.term {
                    indexer2.lock().unwrap().find(&term.as_str(), &mut incoming_reader);
                }
                incoming_reader.get_mut().write_all(Indexer2::ENDING_MSG.as_bytes());
                incoming_reader.get_mut().write(b"\n");
            }
        }
    }
}

fn client_main(args: &Args) {
    let current_dir = std::env::current_dir().unwrap();
    let existing_pipe_name = find_existing_pipe_name(&current_dir.as_path());
    match existing_pipe_name {
        None => {
            println!("Please start the server for the current or parent directory");
        }
        Some(existing_pipe_name) => {
            if let Ok(named_pipe) = LocalSocketStream::connect(existing_pipe_name) {
                let mut pipe_buffer = BufReader::new(named_pipe);
                let mut all_args : Vec<String> = Vec::new();
                for argument in std::env::args() {
                    all_args.push(argument);
                }
                pipe_buffer.get_mut().write_all(all_args.join(" ").as_bytes());
                pipe_buffer.get_mut().write(b"\n");
                {
                    let mut msg = String::with_capacity(128);
                    loop {
                        pipe_buffer.read_line(&mut msg);
                        let trimmed_msg = msg.trim();
                        if trimmed_msg != Indexer2::ENDING_MSG {
                            println!("{trimmed_msg}");
                        } else {
                            break;
                        }
                        msg.clear();
                    }
                }
            }
        }
    }
}

fn main() {
    let args = Args::parse();
    let mut has_named_pipe = false;
    let mode = OperatingMode::Server;
    match args.mode {
        OperatingMode::Server => {
            server_main(&args);
        }
        OperatingMode::Client => {
            client_main(&args);
        }
    }
}
