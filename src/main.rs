use bincode::{
    self,
    config::{self, Config},
    Decode,
    Encode
};
use clap::{Parser, ValueEnum};
use interprocess::local_socket::{LocalSocketListener, LocalSocketStream};
use notify::{
    event::{Event, EventKind},
    RecursiveMode, Result, Watcher,
};
use rand::distributions::Alphanumeric;
use rand::{self, Rng};

use std::{
    cmp::{self},
    collections::hash_map::DefaultHasher,
    collections::HashMap,
    fs::{self, DirEntry},
    hash::Hasher,
    io::{self, BufRead, BufReader, ErrorKind, Read, Write},
    mem::{self},
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
    thread,
};

#[derive(Encode, Decode, ValueEnum, Clone)]
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

#[derive(Encode, Decode, Parser, Clone)]
struct Args {
    #[clap(value_enum, default_value_t = OperatingMode::Client)]
    #[arg(long)]
    mode: OperatingMode,

    #[arg(long)]
    root: Option<String>,

    #[arg(long)]
    client_pipe: Option<String>,

    #[clap(default_value_t = false)]
    #[arg(long)]
    files: bool,

    #[clap(default_value_t = false)]
    #[arg(long, short)]
    word: bool,

    #[clap(default_value_t = false)]
    #[arg(long, short)]
    main_server: bool,

    term: Option<String>,
}

fn write_to_pipe<T : Encode, C: Config>(reader: &mut BufReader<LocalSocketStream>, v: T, config: C) {
    let encoded: Vec<u8> = bincode::encode_to_vec(v, config).unwrap();
    let _ = reader.get_mut().write(&encoded.len().to_ne_bytes());
    let _ = reader.get_mut().write_all(encoded.as_slice());
}

fn read_from_pipe<T: Decode, C: Config>(reader: &mut BufReader<LocalSocketStream>, config: C) -> T {
    let mut struct_len_buffer = [0; mem::size_of::<usize>()];
    let _ = reader.read_exact(&mut struct_len_buffer);
    let struct_len = usize::from_ne_bytes(struct_len_buffer);
    let mut buffer = vec![0u8; struct_len];
    let _ = reader.read_exact(&mut buffer);
    bincode::decode_from_slice(buffer.as_slice(), config).unwrap().0
}

fn convert_path(path: &Path) -> PathBuf {
    let mut hasher = DefaultHasher::new();
    let new_path = PathBuf::from(path.display().to_string().replace("\\", "/"));
    hasher.write(new_path.display().to_string().as_bytes());

    PathBuf::from(hasher.finish().to_string())
}

fn filter_path(filters: &Vec<Filter>, path: &Path, root: &Path, is_dir: bool) -> bool {
    // Ignore files by default, but not dir
    let mut result = is_dir;
    if let Ok(rel_path) = path.strip_prefix(root) {
        let rel_path_str = rel_path.display().to_string();

        for filter in filters {
            let pattern = filter.pattern.as_str();
            // if filter.only_dir && !is_dir {
            //     continue;
            // }
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
    const SERVER_TO_SERVER_ENDING_MSG: &str = "###server_to_server_end###";
    const SERVER_TO_CLIENT_ENDING_MSG: &str = "###server_to_client_end###";
    const MAIN_SERVER_ENDING_MSG: &str = "###main_server_end###";
}

impl Indexer2 {
    fn build(&mut self, path: &Path, filters: &Vec<Filter>) {
        self.root = PathBuf::from(path);

        let mut handles = vec![];
        let thread_count = 4;
        let files_per_thread = 1024;
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
                        let file_count = cmp::min(path_in_queue_count, files_per_thread);
                        for _i in 0..file_count {
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
                files
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
                for _i in 0..files_per_thread {
                    work_queue.paths.push(paths.pop().unwrap());
                }
                cvar.notify_all();
            }
        };

        let _ = visit_dirs(&path, &mut load_files, &self.root.as_path(), &filters);

        {
            let (lock, cvar) = &*pair;
            let mut work_queue = lock.lock().unwrap();
            if paths.len() > 0 {
                work_queue.paths.append(&mut paths);
            }
            work_queue.has_stopped = true;
            // We notify the condvar that the value has changed.
            cvar.notify_all();
        }
        for handle in handles {
            self.files.extend(handle.join().unwrap());
        }
        println!("Indexer2: Done building");
    }

    fn find(&self, args: &Args, reader: &mut BufReader<LocalSocketStream>) {
        if args.term.is_none() {
            return;
        }
        let term = args.term.as_ref().unwrap().as_str();
        for (key, value) in &self.files {
            if value.find(&term).is_some() {
                let mut line_num = 1;
                for line in value.lines() {
                    if line.find(&term).is_some() {
                        let mut found = false;
                        if args.word {
                            let line_bytes = line.as_bytes();
                            for (pos, _) in line.match_indices(term) {
                                if !((pos > 0 && line_bytes[pos - 1].is_ascii_alphanumeric()) || (pos + term.len() < line.len() - 1 && line_bytes[pos + term.len()].is_ascii_alphanumeric())) {
                                    found = true;
                                    break;
                                }
                            }
                        } else {
                            found = true;
                        }
                        if !found {
                            continue;
                        }
                        let _ = reader.get_mut().write_all(format!("{}:{}: {}", key.display().to_string(), line_num, line).as_bytes());
                        let _ = reader.get_mut().write(b"\n");
                    }
                    line_num += 1;
                }
            }
        }
    }

    fn list_files(&self, reader: &mut BufReader<LocalSocketStream>) {
        for (key, _value) in &self.files {
            let _ = reader.get_mut().write_all(format!("{}", key.display().to_string()).as_bytes());
            let _ = reader.get_mut().write(b"\n");
        }
    }

    fn handle_event(&mut self, event: &Event, filters: &Vec<Filter>) {
        match event.kind {
            EventKind::Create(_) | EventKind::Modify(_) => {
                for path in &event.paths {
                    if filter_path(&filters, path, self.root.as_path(), false) && path.is_file() {
                        println!("handle create/modify event: {}", path.display());
                        if let Ok(file_str) = std::fs::read_to_string(path.as_path()) {
                            self.files.insert(PathBuf::clone(path), file_str);
                        }
                    }
                }
            },
            EventKind::Remove(_) => {
                for path in &event.paths {
                    if filter_path(&filters, path, self.root.as_path(), false) && path.is_file() {
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
        if LocalSocketListener::bind(convert_path(&named_pipe_path)).is_err_and(|x| x.kind() == ErrorKind::PermissionDenied) {
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

fn generate_pipe(path: &Path) -> (PathBuf, LocalSocketListener) {
    let out_path;
    let out_pipe;
    loop {
        let rand_str: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .map(char::from)
                .collect();
        let rand_path = convert_path(path.join(rand_str).as_path());
        let pipe = LocalSocketListener::bind(rand_path.as_path());
        if pipe.is_ok() {
            out_path = rand_path;
            out_pipe = pipe.unwrap();
            break;
        }
    }
    (out_path, out_pipe)
}

fn parse_filter(l: &str, filters: &mut Vec<Filter>) {
    let mut line = l;
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

fn server_main(args: &Args) {
    let config = config::standard();
    let root_str = args.root.as_ref().unwrap();
    let path = PathBuf::from(root_str.as_str());
    if let Some(existing_pipe_name) = find_existing_pipe_name(&path) {
        println!("This directory or its parent directory has been indexed: {}", existing_pipe_name.display());
        return;
    }

    println!("Start indexing: {}", path.display());
    let named_pipe = LocalSocketListener::bind(convert_path(path.as_path())).unwrap();

    let mut filters: Vec<Filter> = Vec::new();
    let mut additional_dirs: Vec<PathBuf> = Vec::new();
    let config_path = path.as_path().join(".hanoi");
    if let Ok(config_str) = std::fs::read_to_string(config_path) {
        let mut section = "";
        for line in config_str.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with("#") {
                // Ignore comment
                continue;
            }

            if line.starts_with("[") && line.ends_with("]") {
                section = &line[1..line.len() - 1];
                continue;
            }
            match section {
                "filters" => parse_filter(&line, &mut filters),
                "additional_dirs" => additional_dirs.push(PathBuf::from(line)),
                &_ => println!("Line \"{}\" in an unknown section \"{}\"", line, section),
            }
        }
    }

    let mut indexer2 = Indexer2::default();
    {
        let _scope_time = ScopeTime::default();
        indexer2.build(&path, &filters);
    }
    let indexer2 = Arc::new(Mutex::new(indexer2));
    let mut watcher;
    {
        let indexer2 = indexer2.clone();
        watcher = notify::recommended_watcher(move |res: Result<Event>| {
            match res {
               Ok(event) => indexer2.lock().unwrap().handle_event(&event, &filters),
               Err(e) => println!("watch error: {:?}", e),
            }
        }).unwrap();
    }
    let _ = watcher.watch(&path, RecursiveMode::Recursive);

    let mut child_servers: Vec<Child> = Vec::with_capacity(additional_dirs.len());
    for dir in &additional_dirs {
        let child = Command::new("Hanoi")
            .arg("--mode=server")
            .arg(std::format!("--root={}", dir.display().to_string()))
             .spawn()
             .expect("failed to execute child");
        child_servers.push(child);
    }
    for incoming in named_pipe.incoming() {
        if let Some(stream) = incoming.ok() {
            let mut incoming_reader = BufReader::new(stream);
            let mut client_args : Args = read_from_pipe(&mut incoming_reader, config);
            let pipe_path = PathBuf::from(client_args.client_pipe.as_ref().unwrap());
            if let Ok(client_pipe) = LocalSocketStream::connect(pipe_path.as_path()) {
                let mut client_reader = BufReader::new(client_pipe);
                if client_args.files {
                    indexer2.lock().unwrap().list_files(&mut client_reader);
                } else if client_args.term.is_some() {
                    indexer2.lock().unwrap().find(&client_args, &mut client_reader);
                }
                let _ = client_reader.get_mut().write_all(Indexer2::SERVER_TO_CLIENT_ENDING_MSG.as_bytes());
                let _ = client_reader.get_mut().write(b"\n");
            }
            // Send the arguments to child servers
            let is_main_server = client_args.main_server;
            if is_main_server {
                client_args.main_server = false;
            }
            for dir in &additional_dirs {
                if let Ok(additional_pipe) = LocalSocketStream::connect(convert_path(dir.as_path())) {
                    let mut additional_buffer = BufReader::new(additional_pipe);
                    write_to_pipe(&mut additional_buffer, client_args.clone(), config);
                    loop {
                        let mut msg = String::with_capacity(128);
                        let _ = additional_buffer.read_line(&mut msg);
                        let trimmed_msg = msg.trim();
                        if trimmed_msg == Indexer2::SERVER_TO_SERVER_ENDING_MSG {
                            break;
                        }
                        msg.clear();
                    }
                }
            }
            {
                thread::sleep(Duration::from_millis(1)); // give some time for previous client_pipe to close
            }
            let _ = incoming_reader.get_mut().write_all(Indexer2::SERVER_TO_SERVER_ENDING_MSG.as_bytes());
            let _ = incoming_reader.get_mut().write(b"\n");
            if is_main_server {
                let client_pipe = LocalSocketStream::connect(pipe_path.as_path()).ok().unwrap();
                let mut client_reader = BufReader::new(client_pipe);
                let _ = client_reader.get_mut().write_all(Indexer2::MAIN_SERVER_ENDING_MSG.as_bytes());
                let _ = client_reader.get_mut().write(b"\n");
            }
        }
    }
}

fn client_main(args: &mut Args) {
    let config = config::standard();
    let root_dir = std::env::current_dir().unwrap();
    let existing_pipe_name = find_existing_pipe_name(&root_dir.as_path());
    match existing_pipe_name {
        None => {
            println!("Please start the server for the current or parent directory");
        }
        Some(existing_pipe_name) => {
            let (client_pipe_path, client_pipe) = generate_pipe(existing_pipe_name.as_path());
            if let Ok(named_pipe) = LocalSocketStream::connect(convert_path(existing_pipe_name.as_path())) {
                let mut main_server_reader = BufReader::new(named_pipe);
                args.client_pipe = Some(client_pipe_path.display().to_string());
                args.main_server = true;
                write_to_pipe(&mut main_server_reader, args.clone(), config);
            }

            let mut msg = String::with_capacity(128);
            let mut is_done = false;
            for incoming in client_pipe.incoming() {
                if let Some(stream) = incoming.ok() {
                    let mut incoming_reader = BufReader::new(stream);
                    loop {
                        msg.clear();
                        let _ = incoming_reader.read_line(&mut msg);
                        let trimmed_msg = msg.trim();
                        if trimmed_msg == Indexer2::SERVER_TO_CLIENT_ENDING_MSG {
                            break;
                        } else if trimmed_msg == Indexer2::MAIN_SERVER_ENDING_MSG {
                            is_done = true;
                            break;
                        } else if !trimmed_msg.is_empty() {
                            println!("{trimmed_msg}");
                        }
                    }
                    if is_done {
                        break;
                    }
                }
            }
        }
    }
}

fn main() {
    let mut args = Args::parse();
    match args.mode {
        OperatingMode::Server => {
            server_main(&mut args);
        }
        OperatingMode::Client => {
            client_main(&mut args);
        }
    }
}
