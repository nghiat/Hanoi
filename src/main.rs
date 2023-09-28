use std::io::{self, ErrorKind};
use std::fs::{self, DirEntry};
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::time::Instant;
use interprocess::local_socket::LocalSocketListener;

fn filter_files(dir_entry: &DirEntry) -> bool {
    if let Some(ext) = dir_entry.path().extension() {
        if ext == "cpp" || ext == "h" || ext == "inl" || ext == "cs" {
            return true
        }
    }
    false
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
struct Indexer1 {
    total_len: u64,
    total_files: u64,
    combined_files: String,
    start_indices: Vec<usize>,
    paths: Vec<PathBuf>,
}

pub trait Indexer {
    fn build(&mut self, path: &Path);
    fn find(&self, term: &str);
}

impl Indexer for Indexer1 {
    fn build(&mut self, path: &Path) {
        let mut calculate_metadata = |dir_entry: &DirEntry| {
            if !filter_files(dir_entry) {
                return;
            }
            if let Ok(metadata) = dir_entry.metadata() {
                self.total_len += metadata.len();
                self.total_files += 1;
            }
        };
        let _ = visit_dirs(&path, &mut calculate_metadata);
        self.combined_files.reserve(self.total_len.try_into().unwrap());
        self.start_indices = Vec::with_capacity(self.total_files.try_into().unwrap());
        self.paths = Vec::with_capacity(self.total_files.try_into().unwrap());
        let mut current_start_idx = 0;
        let mut append_to_combined_files = |dir_entry: &DirEntry| {
            if !filter_files(dir_entry) {
                return;
            }
            if let Ok(file_str) = std::fs::read_to_string(dir_entry.path().as_path()) {
                self.combined_files.push_str(&file_str);
                self.start_indices.push(current_start_idx);
                current_start_idx += file_str.len();
                self.paths.push(dir_entry.path());
            }
        };
        let _ = visit_dirs(&path, &mut append_to_combined_files);
        self.start_indices.push(current_start_idx);
        println!("Done indexing");
    }

    fn find(&self, term: &str) {
        let mut start = 0;
        let mut occurrences = Vec::new();
        while let Some(pos) = self.combined_files[start..].find(&term) {
            let found_at = start + pos;
            occurrences.push(found_at);
            start = found_at + term.len();
        }
        let mut o_i = 0;
        for i in 0..self.start_indices.len() - 1 {
            while o_i < occurrences.len() && occurrences[o_i] >= self.start_indices[i] && occurrences[o_i] < self.start_indices[i + 1] {
                // println!("{}", self.paths[i].display());
                o_i += 1;
            }
        }
        println!("Done finding");
    }
}

#[derive(Default)]
struct Indexer2 {
    files: HashMap<PathBuf, String>,
}

impl Indexer for Indexer2 {
    fn build(&mut self, path: &Path) {
        let mut load_files = |dir_entry: &DirEntry| {
            if !filter_files(dir_entry) {
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

    fn find(&self, term: &str) {
        for (key, value) in &self.files {
            if let Some(pos) = value.find(&term) {
                // println!("{}", key.display());
            }
        }
    }
}

fn main() {
    let path = Path::new("D:/projects/UnrealEngine");
    let mut parent_path = path;
    let mut has_named_pipe = false;
    let mut named_pipe : LocalSocketListener;
    loop {
        if LocalSocketListener::bind(parent_path).is_err_and(|x| x.kind() == ErrorKind::PermissionDenied) {
            has_named_pipe = true;
            println!("named_pipe exists");
        }
        let new_parent_path = parent_path.parent();
        match new_parent_path {
            Some(v) => parent_path = v,
            None => break,
        }
    }
    if !has_named_pipe {
        named_pipe = LocalSocketListener::bind(path).unwrap();
    }
    let mut indexer1 = Indexer1::default();
    {
        let scope_time = ScopeTime::default();
        indexer1.build(&path);
    }

    let mut indexer2 = Indexer2::default();
    {
        let scope_time = ScopeTime::default();
        indexer2.build(&path);
    }
    loop {
        let mut term = String::new();
        io::stdin()
            .read_line(&mut term)
            .expect("Failed to read line");

        term = String::from(term.trim());
        {
            let scope_time = ScopeTime::default();
            indexer1.find(&term);
        }
        {
            let scope_time = ScopeTime::default();
            indexer2.find(&term);
        }
    }
}
