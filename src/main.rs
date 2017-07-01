extern crate clap;
extern crate walkdir;
extern crate rocksdb;
extern crate filetime;
extern crate twox_hash;
extern crate bytes;

use bytes::{BufMut, LittleEndian};
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

fn make_fingerprint(path: &Path) -> io::Result<u64> {
    let mut hasher = twox_hash::XxHash::default();
    let mut buf = vec![];
    let mut file = fs::File::open(path)?;

    file.read_to_end(&mut buf)?;

    hasher.write(&buf);
    Ok(hasher.finish())
}

fn get_timestamp(path: &Path) -> io::Result<u64> {
    let metadata = fs::metadata(path)?;

    Ok(FileTime::from_last_modification_time(&metadata).seconds_relative_to_1970())
}

fn make_key(path: &Path) -> &[u8] {
    path.to_str().unwrap().as_bytes()
}

struct FileRecord<'a> {
    fingerprint: u64,
    timestamp: u64,
    key: &'a [u8],
}

impl<'a> FileRecord<'a> {
    fn from_path(path: &'a Path) -> io::Result<Self> {
        Ok(FileRecord{
            fingerprint: make_fingerprint(path)?,
            timestamp: get_timestamp(path)?,
            key: make_key(path)
        })
    }

    fn make_db_value(&self) -> [u8; 16] {
        let bytes = [0 as u8; 16];
        let mut buf = Cursor::new(bytes);

        buf.put_u64::<LittleEndian>(self.fingerprint);
        buf.put_u64::<LittleEndian>(self.timestamp);
        bytes
    }
}

fn add_record(db: &mut DB, path: &Path) -> io::Result<()> {
    let record = FileRecord::from_path(path)?;

    db.put(record.key, &record.make_db_value()).unwrap();
    
    Ok(())
}

fn main() {const VERSION: &'static str = env!("CARGO_PKG_VERSION");

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
    let mut db = DB::open_default(&temp_path).unwrap();

    for entry in WalkDir::new(path) {
        let entry = entry.unwrap();

        let path = &entry.path();
        let extension = path.extension().unwrap_or_default().to_str().unwrap();

        if valid_extensions.contains(extension) {
            //try to retrieve the file's last seen modified date. path is the key
            let key = make_key(path);

            match db.get(key) {
                Ok(Some(value)) => {
                    //found! Compare the times. 

                    //if same, do nothing

                    //otherwise if size or hash differs, store the current date

                    //otherwise put back the old date into the file
                }
                Ok(None) => {
                    //not found at all. Just record this file
                    add_record(&mut db, path).unwrap();
                }
                Err(e) => println!("operational problem encountered: {}", e),
            }
        }
    }


}
