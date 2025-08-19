// File: src/cache.rs
use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use bincode;
use std::time::SystemTime;

use crate::utils::{TimsTOFRawData, IndexedTimsTOFData};

pub struct CacheManager {
    cache_dir: PathBuf,
}

impl CacheManager {
    pub fn new() -> Self {
        let cache_dir = PathBuf::from(".timstof_cache");
        fs::create_dir_all(&cache_dir).unwrap();
        Self { cache_dir }
    }
    
    fn get_cache_path(&self, source_path: &Path, cache_type: &str) -> PathBuf {
        let source_name = source_path.file_name().unwrap().to_str().unwrap();
        let cache_name = format!("{}.{}.cache", source_name, cache_type);
        self.cache_dir.join(cache_name)
    }
    
    fn get_metadata_path(&self, source_path: &Path) -> PathBuf {
        let source_name = source_path.file_name().unwrap().to_str().unwrap();
        let meta_name = format!("{}.meta", source_name);
        self.cache_dir.join(meta_name)
    }
    
    pub fn is_cache_valid(&self, source_path: &Path) -> bool {
        let ms1_cache_path = self.get_cache_path(source_path, "ms1_indexed");
        let ms2_cache_path = self.get_cache_path(source_path, "ms2_indexed");
        let meta_path = self.get_metadata_path(source_path);
        
        if !ms1_cache_path.exists() || !ms2_cache_path.exists() || !meta_path.exists() {
            return false;
        }
        
        // Check source folder modification time
        let source_modified = fs::metadata(source_path)
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);
            
        let cache_modified = fs::metadata(&ms1_cache_path)
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);
            
        cache_modified > source_modified
    }
    
    pub fn save_indexed_data(
        &self, 
        source_path: &Path, 
        ms1_indexed: &IndexedTimsTOFData,
        ms2_indexed_pairs: &Vec<((f32, f32), IndexedTimsTOFData)>
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("Saving indexed data to cache...");
        let start_time = std::time::Instant::now();
        
        // Save MS1 indexed data
        let ms1_cache_path = self.get_cache_path(source_path, "ms1_indexed");
        let ms1_file = File::create(&ms1_cache_path)?;
        let ms1_writer = BufWriter::with_capacity(1024 * 1024 * 64, ms1_file);
        bincode::serialize_into(ms1_writer, ms1_indexed)?;
        
        // Save MS2 indexed data
        let ms2_cache_path = self.get_cache_path(source_path, "ms2_indexed");
        let ms2_file = File::create(&ms2_cache_path)?;
        let ms2_writer = BufWriter::with_capacity(1024 * 1024 * 64, ms2_file);
        bincode::serialize_into(ms2_writer, ms2_indexed_pairs)?;
        
        // Save metadata - simplified without get_total_points()
        let meta_path = self.get_metadata_path(source_path);
        let metadata = format!(
            "cached at: {:?}\nms2_windows: {}\ntype: indexed",
            SystemTime::now(),
            ms2_indexed_pairs.len()
        );
        fs::write(meta_path, metadata)?;
        
        let elapsed = start_time.elapsed();
        let ms1_size = fs::metadata(&ms1_cache_path)?.len();
        let ms2_size = fs::metadata(&ms2_cache_path)?.len();
        let total_size_mb = (ms1_size + ms2_size) as f32 / 1024.0 / 1024.0;
        println!("Indexed cache saved: {:.2} MB total, time: {:.2}s", 
                 total_size_mb, elapsed.as_secs_f32());
        Ok(())
    }
    
    pub fn load_indexed_data(
        &self, 
        source_path: &Path
    ) -> Result<(IndexedTimsTOFData, Vec<((f32, f32), IndexedTimsTOFData)>), Box<dyn std::error::Error>> {
        println!("Loading indexed data from cache...");
        let start_time = std::time::Instant::now();
        
        // Load MS1 indexed data
        let ms1_cache_path = self.get_cache_path(source_path, "ms1_indexed");
        let ms1_file = File::open(&ms1_cache_path)?;
        let ms1_reader = BufReader::with_capacity(1024 * 1024 * 64, ms1_file);
        let ms1_indexed = bincode::deserialize_from(ms1_reader)?;
        
        // Load MS2 indexed data
        let ms2_cache_path = self.get_cache_path(source_path, "ms2_indexed");
        let ms2_file = File::open(&ms2_cache_path)?;
        let ms2_reader = BufReader::with_capacity(1024 * 1024 * 64, ms2_file);
        let ms2_indexed_pairs = bincode::deserialize_from(ms2_reader)?;
        
        let elapsed = start_time.elapsed();
        println!("Indexed cache loaded (time: {:.2}s)", elapsed.as_secs_f32());
        Ok((ms1_indexed, ms2_indexed_pairs))
    }
    
    pub fn clear_cache(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.cache_dir.exists() {
            fs::remove_dir_all(&self.cache_dir)?;
            println!("Cache cleared");
        }
        Ok(())
    }
    
    pub fn get_cache_info(&self) -> Result<Vec<(String, u32, String)>, Box<dyn std::error::Error>> {
        let mut info = Vec::new();
        
        if self.cache_dir.exists() {
            for entry in fs::read_dir(&self.cache_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("cache") {
                    let metadata = fs::metadata(&path)?;
                    let size = metadata.len() as u32;
                    let name = path.file_name().unwrap().to_str().unwrap().to_string();
                    let size_mb = size as f32 / 1024.0 / 1024.0;
                    let size_gb = size as f32 / 1024.0 / 1024.0 / 1024.0;
                    
                    let size_str = if size_gb >= 1.0 {
                        format!("{:.2} GB", size_gb)
                    } else {
                        format!("{:.2} MB", size_mb)
                    };
                    
                    info.push((name, size, size_str));
                }
            }
        }
        
        Ok(info)
    }
}