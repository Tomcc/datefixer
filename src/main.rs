extern crate clap;
extern crate walkdir;
extern crate rocksdb;
extern crate filetime;
extern crate twox_hash;
extern crate bytes;
extern crate threadpool;
extern crate num_cpus;

use threadpool::ThreadPool;
use bytes::{BufMut, LittleEndian, Buf};
use clap::{Arg, App};
use std::path::{Path, PathBuf};
use std::process;
use std::env;
use walkdir::WalkDir;
use std::collections::HashSet;
use rocksdb::DB;
use std::fs;
use std::io::prelude::*;
use std::io;
use std::io::Cursor;
use std::hash::Hasher;
use filetime::FileTime;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Condvar;
use std::cell::RefCell;

thread_local!{
    static READ_BUFFER: RefCell<Vec<u8>> = RefCell::new(vec![]);
}

fn make_fingerprint(path: &Path) -> io::Result<(u64, u64)> {
    let mut hasher = twox_hash::XxHash::default();
    let mut file = fs::File::open(path)?;
    let mut len = 0 as u64;

    READ_BUFFER.with(|buf_rc| {
        let mut buf = buf_rc.borrow_mut();
        buf.clear();
        file.read_to_end(&mut buf).unwrap();

        hasher.write(&buf);
        len = buf.len() as u64;
    });

    Ok((hasher.finish(), len))
}

fn timestamp_from_metadata(metadata: &fs::Metadata) -> u64 {
    FileTime::from_last_modification_time(&metadata).seconds_relative_to_1970()
}
fn get_timestamp(path: &Path) -> io::Result<u64> {
    let metadata = fs::metadata(path)?;

    Ok(timestamp_from_metadata(&metadata))
}

fn make_key(path: &Path) -> &[u8] {
    path.to_str().unwrap().as_bytes()
}

#[derive(Debug)]
struct FileRecord {
    fingerprint: u64,
    timestamp: u64,
    size: u64,
}

type BinaryDBValue = [u8; 8 * 3];

impl FileRecord {
    fn from_path(path: &Path) -> io::Result<Self> {
        let (fingerprint, size) = make_fingerprint(path)?;
        Ok(FileRecord{
            fingerprint: fingerprint,
            timestamp: get_timestamp(path)?,
            size: size,
        })
    }

    fn from_db(value: &[u8]) -> Self {
        let mut buf = Cursor::new(value);
        FileRecord {
            fingerprint: buf.get_u64::<LittleEndian>(),
            timestamp: buf.get_u64::<LittleEndian>(),
            size: buf.get_u64::<LittleEndian>(),
        }
    }

    fn make_db_value(&self) -> BinaryDBValue {
        let mut buf = Cursor::new(BinaryDBValue::default());

        buf.put_u64::<LittleEndian>(self.fingerprint);
        buf.put_u64::<LittleEndian>(self.timestamp);
        buf.put_u64::<LittleEndian>(self.size);        
        
        buf.into_inner()
    }
}

fn add_record(db: &DB, path: &Path) -> io::Result<()> {
    let record = FileRecord::from_path(path)?;

    db.put(make_key(path), &record.make_db_value()).unwrap();
    
    Ok(())
}

fn main() {
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    let matches = App::new("DateFixer")
        .version(VERSION)
        .about("Restores the last modified date on files that haven't changed (or changed back) since then")
        .arg(Arg::with_name("root_path")
            .help("The root folder to fix")
            .value_name("Path")
            .takes_value(true)
            .required(true))
        .get_matches();

        
    let path = PathBuf::from(matches.value_of("root_path").unwrap());
    
    if !path.is_dir() {
        println!("{} is not a file, or couldn't be found!", path.display());
        process::exit(1);
    }

    println!("Running datefixer in {} recursively", path.display());

    //TODO make parameter
    let valid_extensions: HashSet<&str> = ["cpp", "cc", "h", "hpp", "mm", "java"].iter().cloned().collect();

    //create our temp dir
    let mut temp_path = env::temp_dir();
    temp_path.push("datafixer");
    fs::create_dir_all(&temp_path).unwrap();    

    //open or create the database
    let db = Arc::new(DB::open_default(&temp_path).unwrap());

    //create other thread management stuff
    let pool = ThreadPool::new(num_cpus::get() * 2);
    let count = Arc::new(Mutex::new(0));
    let signal = Arc::new(Condvar::new());
    let mut total_tasks = 0;

    for entry in WalkDir::new(path) {
        let entry = entry.unwrap();

        let path = entry.path().to_owned();
        let extension = entry.path().extension().unwrap_or_default().to_str().unwrap();

        if valid_extensions.contains(extension) {

            let db = db.clone();
            let count = count.clone();
            let signal = signal.clone();
            total_tasks += 1;

            pool.execute(move || {
            //try to retrieve the file's last seen modified date. path is the key
                let key = make_key(&path);
                match db.get(key) {
                    Ok(Some(value)) => {
                        //found! Get a record and compare the times. 
                        let file_record = FileRecord::from_db(&value);
                        let new_timestamp = get_timestamp(&path).unwrap();
                        
                        if file_record.timestamp != new_timestamp {
                            //timestamps are different...
                            let (fingerprint, new_size) = make_fingerprint(&path).unwrap();

                            if new_size != file_record.size || fingerprint != file_record.fingerprint {
                                //size changed, or fingerprint changed, it can't be the same. make a new record to write out
                                let new_record = FileRecord{
                                    size: new_size,
                                    timestamp: new_timestamp,
                                    fingerprint: fingerprint,
                                };
                                db.put(&key, &new_record.make_db_value()).unwrap();

                                println!("Updated {} size changed: {} - hash changed: {}", path.display(), new_size != file_record.size, fingerprint != file_record.fingerprint);
                            }
                            else {
                                //ok, everything was the same, only the timestamp changed. Roll it back!
                                
                                let metadata = fs::metadata(&path).unwrap();
                                let atime = FileTime::from_creation_time(&metadata).unwrap();
                                let mtime = FileTime::from_seconds_since_1970(file_record.timestamp, 0);
                                filetime::set_file_times(&path, atime, mtime).unwrap();
                                
                                println!("Fixed {}", path.display());
                            }
                        }
                    }
                    Ok(None) => {
                        //not found at all. Just record this file
                        add_record(&db, &path).unwrap();
                    }
                    Err(e) => println!("Database error!\n {}", e),
                }

                //signal done
                *count.as_ref().lock().unwrap() += 1;
                signal.as_ref().notify_one();
            });
        }
    }

    //wait on everything being done
    let mut count = count.lock().unwrap();
    // As long as the value inside the `Mutex` is false, we wait.
    while *count < total_tasks {
        count = signal.wait(count).unwrap();
    }
}
