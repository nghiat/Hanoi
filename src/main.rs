use clap::{Parser, ValueEnum};
use std::io::{self, BufRead, BufReader, ErrorKind};
use std::fs::{self, DirEntry};
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::time::Instant;
use interprocess::local_socket::{LocalSocketListener, LocalSocketStream};
use std::io::Read;
use std::io::Write;
use std::cmp::min;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use notify::{
    event::{self, EventKind},
    Watcher, RecommendedWatcher, RecursiveMode, Result
};

#[derive(ValueEnum, Clone)]
enum OperatingMode {
    Server,
    Client,
}

struct Filter {
    should_include: bool,
    should_start_with: bool,
    should_end_with: bool,
    only_dir: bool,
    pattern: String,
}

struct WorkQueue {
    paths: Vec<PathBuf>,
    has_stopped: bool,
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

fn filter_path(filters: &Vec<Filter>, path: &Path, root: &Path, is_dir: bool) -> bool {
    // Ignore files by default, but not dir
    let mut result = is_dir;
    if let Ok(rel_path) = path.strip_prefix(root) {
        let rel_path_str = rel_path.display().to_string();

        for filter in filters {
            let pattern = filter.pattern.as_str();
            if filter.only_dir && !is_dir {
                continue;
            }
            if filter.should_start_with && filter.should_end_with {
                if pattern == rel_path_str {
                    result = filter.should_include;
                }
            } else if filter.should_start_with || filter.should_end_with {
                if filter.should_start_with && rel_path_str.starts_with(pattern) {
                    result = filter.should_include;
                } else if filter.should_end_with && rel_path_str.ends_with(pattern) {
                    result = filter.should_include;
                }
            } else {
                if rel_path_str.contains(pattern) {
                    result = filter.should_include;
                }
            }
        }
    }
    result
}

fn visit_dirs(dir: &Path, cb: &mut impl FnMut(&DirEntry), root: &Path, filters: &Vec<Filter>) -> io::Result<()> {
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                if filter_path(&filters, path.as_path(), &root, true) {
                    visit_dirs(&path, cb, &root, &filters)?;
                }
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
}

impl Indexer2 {
    const ENDING_MSG: &str = "###end###";
}

impl Indexer2 {
    fn build(&mut self, path: &Path, filters: &Vec<Filter>) {
        self.root = PathBuf::from(path);

        let mut handles = vec![];
        let thread_count = 10;
        let files_per_thread = 16;
        let work_queue = WorkQueue {
            paths: Vec::with_capacity(thread_count * files_per_thread),
            has_stopped: false,
        };
        let pair = Arc::new((Mutex::new(work_queue), Condvar::new()));
        for _ in 0..thread_count {
            let pair2 = Arc::clone(&pair);
            let handle = thread::spawn(move || {
                let mut files: HashMap<PathBuf, String> = Default::default();
                let mut paths: Vec<PathBuf> = Vec::with_capacity(files_per_thread);
                let (lock, cvar) = &*pair2;
                loop {
                    let mut work_queue = lock.lock().unwrap();
                    while !work_queue.has_stopped && work_queue.paths.is_empty() {
                        work_queue = cvar.wait(work_queue).unwrap();
                    }
                    let path_in_queue_count = work_queue.paths.len();
                    if path_in_queue_count > 0 {
                        let file_count = min(path_in_queue_count, files_per_thread);
                        for i in 0..file_count {
                            paths.push(work_queue.paths.pop().unwrap());
                        }
                    }
                    let should_stopped = work_queue.has_stopped && work_queue.paths.is_empty();
                    drop(work_queue);
                    for path in &paths {
                        if let Ok(file_str) = std::fs::read_to_string(path) {
                            files.insert(PathBuf::from(path), file_str);
                        }
                    }
                    if should_stopped {
                        break;
                    }
                }
            });
            handles.push(handle);
        }

        let mut paths = Vec::<PathBuf>::with_capacity(thread_count * files_per_thread);
        let mut load_files = |dir_entry: &DirEntry| {
            if !filter_path(&filters, dir_entry.path().as_path(), &path, false) {
                return;
            }

            paths.push(dir_entry.path());
            if paths.len() > files_per_thread {
                let (lock, cvar) = &*pair;
                let mut work_queue = lock.lock().unwrap();
                for i in 0..files_per_thread {
                    work_queue.paths.push(paths.pop().unwrap());
                }
                cvar.notify_all();
            }
        };

        let _ = visit_dirs(&path, &mut load_files, &self.root.as_path(), &filters);

        let (lock, cvar) = &*pair;
        let mut work_queue = lock.lock().unwrap();
        if paths.len() > 0 {
            work_queue.paths.append(&mut paths);
        }
        work_queue.has_stopped = true;
        // We notify the condvar that the value has changed.
        cvar.notify_all();
        for handle in handles {
            handle.join().unwrap();
        }
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

    fn handle_event(&mut self, event: &event::Event, filters: &Vec<Filter>) {
        match event.kind {
            EventKind::Create(_) | EventKind::Modify(_) => {
                for path in &event.paths {
                    if filter_path(&filters, path, self.root.as_path(), true) && path.is_file() {
                        println!("handle create event: {}", path.display());
                        if let Ok(file_str) = std::fs::read_to_string(path.as_path()) {
                            self.files.insert(PathBuf::clone(path), file_str);
                        }
                    }
                }
            },
            EventKind::Remove(remove) => {
                for path in &event.paths {
                    if filter_path(&filters, path, self.root.as_path(), true) && path.is_file() {
                        println!("handle remove event: {}", path.display());
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

    let mut filters: Vec<Filter> = Vec::new();
    let config_path = Path::new(path).join(".hanoi");
    if let Ok(config_str) = std::fs::read_to_string(config_path) {
        for line in config_str.lines() {
            let mut line = line.trim();
            if line.is_empty() || line.starts_with("#") {
                // Ignore comment
                continue;
            }

            let mut filter = Filter {
                should_include : true,
                should_start_with : true,
                should_end_with : true,
                only_dir : false,
                pattern : String::new(),
            };
            if line.starts_with("!") {
                filter.should_include = false;
                line = &line[1..]
            }
            if line.starts_with("*") {
                filter.should_start_with = false;
                line = &line[1..]
            }
            if line.ends_with("*") {
                filter.should_end_with = false;
                line = &line[0..line.len() - 1]
            }
            if line.ends_with("/") {
                filter.only_dir = true;
                line = &line[0..line.len() - 1]
            }
            let mut pattern = String::from(line);
            if cfg!(target_os = "windows") {
                pattern = pattern.replace("/", "\\");
            } else {
                pattern = pattern.replace("\\", "/");
            }
            filter.pattern = pattern;
            filters.push(filter);
        }
    }

    let mut indexer2 = Indexer2::default();
    {
        let scope_time = ScopeTime::default();
        indexer2.build(&path, &filters);
    }
    let indexer2 = Arc::new(Mutex::new(indexer2));
    let mut watcher;
    {
        let indexer2 = indexer2.clone();
        watcher = notify::recommended_watcher(move |res: Result<event::Event>| {
            match res {
               Ok(event) => indexer2.lock().unwrap().handle_event(&event, &filters),
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
