#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::Instant;
use std::env;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};

use rayon::prelude::*;
use timsrust::{converters::ConvertableDomain, readers::{FrameReader, MetadataReader}, MSLevel};
use timsrust::converters::{Tof2MzConverter, Scan2ImConverter};
use serde::{Serialize, Deserialize};
use ahash::RandomState;

const NUM_THREADS: usize = 64;

// ============================================================================
// Core Data Structures
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimsTOFData {
    pub rt_values_min: Vec<f32>,
    pub mobility_values: Vec<f32>,
    pub mz_values: Vec<f32>,
    pub intensity_values: Vec<u32>,
    pub frame_indices: Vec<u32>,
    pub scan_indices: Vec<u32>,
}

impl TimsTOFData {
    pub fn new() -> Self {
        TimsTOFData {
            rt_values_min: Vec::new(),
            mobility_values: Vec::new(),
            mz_values: Vec::new(),
            intensity_values: Vec::new(),
            frame_indices: Vec::new(),
            scan_indices: Vec::new(),
        }
    }
    
    pub fn with_capacity(capacity: usize) -> Self {
        let aligned_cap = ((capacity + 15) / 16) * 16;
        Self {
            rt_values_min: Vec::with_capacity(aligned_cap),
            mobility_values: Vec::with_capacity(aligned_cap),
            mz_values: Vec::with_capacity(aligned_cap),
            intensity_values: Vec::with_capacity(aligned_cap),
            frame_indices: Vec::with_capacity(aligned_cap),
            scan_indices: Vec::with_capacity(aligned_cap),
        }
    }
    
    pub fn preallocate_exact(capacity: usize) -> Self {
        let mut data = Self::with_capacity(capacity);
        data.rt_values_min.reserve_exact(capacity);
        data.mobility_values.reserve_exact(capacity);
        data.mz_values.reserve_exact(capacity);
        data.intensity_values.reserve_exact(capacity);
        data.frame_indices.reserve_exact(capacity);
        data.scan_indices.reserve_exact(capacity);
        data
    }
    
    #[inline(always)]
    fn merge_from(&mut self, other: TimsTOFData) {
        self.rt_values_min.extend(other.rt_values_min);
        self.mobility_values.extend(other.mobility_values);
        self.mz_values.extend(other.mz_values);
        self.intensity_values.extend(other.intensity_values);
        self.frame_indices.extend(other.frame_indices);
        self.scan_indices.extend(other.scan_indices);
    }
    
    #[inline(always)]
    unsafe fn append_unchecked(&mut self, other: &mut Self) {
        let len = self.rt_values_min.len();
        let other_len = other.rt_values_min.len();
        let new_len = len + other_len;
        
        self.rt_values_min.reserve(other_len);
        self.mobility_values.reserve(other_len);
        self.mz_values.reserve(other_len);
        self.intensity_values.reserve(other_len);
        self.frame_indices.reserve(other_len);
        self.scan_indices.reserve(other_len);
        
        std::ptr::copy_nonoverlapping(
            other.rt_values_min.as_ptr(),
            self.rt_values_min.as_mut_ptr().add(len),
            other_len
        );
        std::ptr::copy_nonoverlapping(
            other.mobility_values.as_ptr(),
            self.mobility_values.as_mut_ptr().add(len),
            other_len
        );
        std::ptr::copy_nonoverlapping(
            other.mz_values.as_ptr(),
            self.mz_values.as_mut_ptr().add(len),
            other_len
        );
        std::ptr::copy_nonoverlapping(
            other.intensity_values.as_ptr(),
            self.intensity_values.as_mut_ptr().add(len),
            other_len
        );
        std::ptr::copy_nonoverlapping(
            other.frame_indices.as_ptr(),
            self.frame_indices.as_mut_ptr().add(len),
            other_len
        );
        std::ptr::copy_nonoverlapping(
            other.scan_indices.as_ptr(),
            self.scan_indices.as_mut_ptr().add(len),
            other_len
        );
        
        self.rt_values_min.set_len(new_len);
        self.mobility_values.set_len(new_len);
        self.mz_values.set_len(new_len);
        self.intensity_values.set_len(new_len);
        self.frame_indices.set_len(new_len);
        self.scan_indices.set_len(new_len);
    }
}

impl Default for TimsTOFData {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimsTOFRawData {
    pub ms1_data: TimsTOFData,
    pub ms2_windows: Vec<((f32, f32), TimsTOFData)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexedTimsTOFData {
    pub rt_values_min: Vec<f32>,
    pub mobility_values: Vec<f32>,
    pub mz_values: Vec<f32>,
    pub intensity_values: Vec<u32>,
    pub frame_indices: Vec<u32>,
    pub scan_indices: Vec<u32>,
}

// ============================================================================
// Save/Load Functionality
// ============================================================================

#[derive(Debug, Clone, Copy)]
pub enum SaveFormat {
    Json,
    Bincode,
    Csv,
}

impl SaveFormat {
    fn extension(&self) -> &str {
        match self {
            SaveFormat::Json => "json",
            SaveFormat::Bincode => "bin",
            SaveFormat::Csv => "csv",
        }
    }
}

pub struct DataSaver {
    output_dir: PathBuf,
    format: SaveFormat,
}

impl DataSaver {
    pub fn new(output_dir: PathBuf, format: SaveFormat) -> Result<Self, Box<dyn Error>> {
        // Create output directory if it doesn't exist
        fs::create_dir_all(&output_dir)?;
        Ok(Self { output_dir, format })
    }
    
    pub fn save_indexed_data(
        &self,
        ms1_data: &IndexedTimsTOFData,
        ms2_data: &[((f32, f32), IndexedTimsTOFData)],
        base_name: &str,
    ) -> Result<(), Box<dyn Error>> {
        let save_start = Instant::now();
        
        match self.format {
            SaveFormat::Json => self.save_as_json(ms1_data, ms2_data, base_name)?,
            SaveFormat::Bincode => self.save_as_bincode(ms1_data, ms2_data, base_name)?,
            SaveFormat::Csv => self.save_as_csv(ms1_data, ms2_data, base_name)?,
        }
        
        println!("  Data saved in {:.3}s", save_start.elapsed().as_secs_f32());
        Ok(())
    }
    
    fn save_as_json(
        &self,
        ms1_data: &IndexedTimsTOFData,
        ms2_data: &[((f32, f32), IndexedTimsTOFData)],
        base_name: &str,
    ) -> Result<(), Box<dyn Error>> {
        // Save MS1 data
        let ms1_path = self.output_dir.join(format!("{}_ms1.json", base_name));
        let ms1_file = File::create(&ms1_path)?;
        let ms1_writer = BufWriter::new(ms1_file);
        serde_json::to_writer(ms1_writer, ms1_data)?;
        println!("  Saved MS1 data to: {:?}", ms1_path);
        
        // Save MS2 data
        let ms2_path = self.output_dir.join(format!("{}_ms2.json", base_name));
        let ms2_file = File::create(&ms2_path)?;
        let ms2_writer = BufWriter::new(ms2_file);
        serde_json::to_writer(ms2_writer, ms2_data)?;
        println!("  Saved MS2 data to: {:?}", ms2_path);
        
        Ok(())
    }
    
    fn save_as_bincode(
        &self,
        ms1_data: &IndexedTimsTOFData,
        ms2_data: &[((f32, f32), IndexedTimsTOFData)],
        base_name: &str,
    ) -> Result<(), Box<dyn Error>> {
        // Save MS1 data
        let ms1_path = self.output_dir.join(format!("{}_ms1.bin", base_name));
        let ms1_file = File::create(&ms1_path)?;
        let ms1_writer = BufWriter::new(ms1_file);
        bincode::serialize_into(ms1_writer, ms1_data)?;
        println!("  Saved MS1 data to: {:?}", ms1_path);
        
        // Save MS2 data
        let ms2_path = self.output_dir.join(format!("{}_ms2.bin", base_name));
        let ms2_file = File::create(&ms2_path)?;
        let ms2_writer = BufWriter::new(ms2_file);
        bincode::serialize_into(ms2_writer, ms2_data)?;
        println!("  Saved MS2 data to: {:?}", ms2_path);
        
        Ok(())
    }
    
    fn save_as_csv(
        &self,
        ms1_data: &IndexedTimsTOFData,
        ms2_data: &[((f32, f32), IndexedTimsTOFData)],
        base_name: &str,
    ) -> Result<(), Box<dyn Error>> {
        // Save MS1 data as CSV
        let ms1_path = self.output_dir.join(format!("{}_ms1.csv", base_name));
        self.write_csv(&ms1_path, ms1_data, None)?;
        println!("  Saved MS1 data to: {:?}", ms1_path);
        
        // Save MS2 data as separate CSV files for each window
        for (idx, ((low, high), data)) in ms2_data.iter().enumerate() {
            let ms2_path = self.output_dir.join(
                format!("{}_ms2_window_{}_{}_{}.csv", base_name, idx, low, high)
            );
            self.write_csv(&ms2_path, data, Some((*low, *high)))?;
        }
        println!("  Saved {} MS2 window files", ms2_data.len());
        
        Ok(())
    }
    
    fn write_csv(
        &self, 
        path: &Path, 
        data: &IndexedTimsTOFData,
        window: Option<(f32, f32)>
    ) -> Result<(), Box<dyn Error>> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        
        // Write header
        if let Some((low, high)) = window {
            writeln!(writer, "# MS2 Window: {:.4} - {:.4}", low, high)?;
        }
        writeln!(writer, "rt_min,mobility,mz,intensity,frame_index,scan_index")?;
        
        // Write data
        for i in 0..data.mz_values.len() {
            writeln!(
                writer,
                "{},{},{},{},{},{}",
                data.rt_values_min[i],
                data.mobility_values[i],
                data.mz_values[i],
                data.intensity_values[i],
                data.frame_indices[i],
                data.scan_indices[i]
            )?;
        }
        
        Ok(())
    }
}

pub fn load_indexed_data(
    input_dir: &Path,
    base_name: &str,
    format: SaveFormat,
) -> Result<(IndexedTimsTOFData, Vec<((f32, f32), IndexedTimsTOFData)>), Box<dyn Error>> {
    match format {
        SaveFormat::Json => {
            let ms1_path = input_dir.join(format!("{}_ms1.json", base_name));
            let ms1_file = File::open(ms1_path)?;
            let ms1_data: IndexedTimsTOFData = serde_json::from_reader(ms1_file)?;
            
            let ms2_path = input_dir.join(format!("{}_ms2.json", base_name));
            let ms2_file = File::open(ms2_path)?;
            let ms2_data: Vec<((f32, f32), IndexedTimsTOFData)> = serde_json::from_reader(ms2_file)?;
            
            Ok((ms1_data, ms2_data))
        }
        SaveFormat::Bincode => {
            let ms1_path = input_dir.join(format!("{}_ms1.bin", base_name));
            let ms1_file = File::open(ms1_path)?;
            let ms1_data: IndexedTimsTOFData = bincode::deserialize_from(ms1_file)?;
            
            let ms2_path = input_dir.join(format!("{}_ms2.bin", base_name));
            let ms2_file = File::open(ms2_path)?;
            let ms2_data: Vec<((f32, f32), IndexedTimsTOFData)> = bincode::deserialize_from(ms2_file)?;
            
            Ok((ms1_data, ms2_data))
        }
        SaveFormat::Csv => {
            Err("Loading from CSV format is not implemented".into())
        }
    }
}

// ============================================================================
// Data Loading - Optimized Version
// ============================================================================

#[derive(Clone)]
struct FrameProcessor {
    mz_cv: Arc<Tof2MzConverter>,
    im_cv: Arc<Scan2ImConverter>,
}

impl FrameProcessor {
    #[inline(always)]
    fn compute_scan_ids(scan_offsets: &[usize], len: usize) -> Vec<u32> {
        let mut ids = vec![0u32; len];
        if scan_offsets.is_empty() {
            return ids;
        }
        
        for scan in 0..scan_offsets.len() - 1 {
            let start = scan_offsets[scan];
            let end = scan_offsets[scan + 1];
            for i in start..end.min(len) {
                ids[i] = scan as u32;
            }
        }
        
        // Handle the last scan
        let last_scan = scan_offsets.len() - 1;
        for i in scan_offsets[last_scan]..len {
            ids[i] = last_scan as u32;
        }
        
        ids
    }
    
    #[inline(always)]
    fn process_peaks_batch_optimized(
        &self,
        tof_indices: &[u32],
        intensities: &[u32],
        scan_offsets: &[usize],
        rt_min: f32,
        frame_index: u32,
        scan_filter: Option<(usize, usize)>,
    ) -> TimsTOFData {
        let n_peaks = tof_indices.len();
        if n_peaks == 0 {
            return TimsTOFData::new();
        }
        
        // Pre-compute scan IDs once - O(n) instead of O(n*m)
        let scan_ids = Self::compute_scan_ids(scan_offsets, n_peaks);
        
        // Pre-allocate with exact capacity
        let mut data = TimsTOFData::with_capacity(n_peaks);
        
        // Process in chunks for better cache locality
        for (idx, ((&tof, &intensity), &scan_id)) in tof_indices.iter()
            .zip(intensities.iter())
            .zip(scan_ids.iter())
            .enumerate() 
        {
            if let Some((start, end)) = scan_filter {
                if (scan_id as usize) < start || (scan_id as usize) > end { 
                    continue; 
                }
            }
            
            let mz = self.mz_cv.convert(tof as f64) as f32;
            let im = self.im_cv.convert(scan_id as f64) as f32;
            
            data.rt_values_min.push(rt_min);
            data.mobility_values.push(im);
            data.mz_values.push(mz);
            data.intensity_values.push(intensity);
            data.frame_indices.push(frame_index);
            data.scan_indices.push(scan_id);
        }
        
        data
    }
}

#[derive(Default)]
struct LocalResult {
    ms1: TimsTOFData,
    ms2: HashMap<(u32, u32), TimsTOFData, RandomState>,
}

fn estimate_total_peaks(frames: &FrameReader) -> (usize, usize) {
    let sample_size = std::cmp::min(50, frames.len());
    let mut ms1_sum = 0;
    let mut ms2_sum = 0;
    let mut ms1_count = 0;
    let mut ms2_count = 0;
    
    for idx in 0..sample_size {
        if let Ok(frame) = frames.get(idx) {
            match frame.ms_level {
                MSLevel::MS1 => {
                    ms1_sum += frame.tof_indices.len();
                    ms1_count += 1;
                }
                MSLevel::MS2 => {
                    ms2_sum += frame.tof_indices.len();
                    ms2_count += 1;
                }
                _ => {}
            }
        }
    }
    
    let avg_ms1 = if ms1_count > 0 { ms1_sum / ms1_count } else { 10000 };
    let avg_ms2 = if ms2_count > 0 { ms2_sum / ms2_count } else { 10000 };
    
    let total_frames = frames.len();
    let estimated_ms1 = avg_ms1 * (total_frames * 2 / 5);
    let estimated_ms2 = avg_ms2 * (total_frames * 3 / 5);
    
    (estimated_ms1, estimated_ms2)
}

pub fn read_timstof_data(d_folder: &Path) -> Result<TimsTOFRawData, Box<dyn Error>> {
    let total_start = Instant::now();
    
    println!("Initializing metadata readers...");
    let meta_start = Instant::now();
    let tdf_path = d_folder.join("analysis.tdf");
    let meta = MetadataReader::new(&tdf_path)?;
    let mz_cv = Arc::new(meta.mz_converter);
    let im_cv = Arc::new(meta.im_converter);
    println!("  Metadata initialization: {:.3}s", meta_start.elapsed().as_secs_f32());
    
    println!("Initializing frame reader...");
    let frame_reader_start = Instant::now();
    let frames = Arc::new(FrameReader::new(d_folder)?);
    let n_frames = frames.len();
    println!("  Frame reader initialization: {:.3}s", frame_reader_start.elapsed().as_secs_f32());
    println!("  Total frames to process: {}", n_frames);
    
    println!("Estimating data size for pre-allocation...");
    let (ms1_estimate, _) = estimate_total_peaks(&frames);
    println!("  Estimated MS1 peaks: ~{}", ms1_estimate);
    
    println!("Processing frames with {} threads...", NUM_THREADS);
    let process_start = Instant::now();
    
    let processed_count = Arc::new(AtomicUsize::new(0));
    let processed_clone = Arc::clone(&processed_count);
    
    // Pure parallel reduction without aggregator thread
    let result = (0..n_frames)
        .into_par_iter()
        .map_init(
            || FrameProcessor { 
                mz_cv: Arc::clone(&mz_cv), 
                im_cv: Arc::clone(&im_cv) 
            },
            |processor, idx| {
                let frame = match frames.get(idx) {
                    Ok(f) => f,
                    Err(_) => return LocalResult::default(),
                };
                
                // Early return for empty frames
                if frame.tof_indices.is_empty() {
                    processed_clone.fetch_add(1, AtomicOrdering::Relaxed);
                    return LocalResult::default();
                }
                
                let rt_min = frame.rt_in_seconds as f32 / 60.0;
                let mut out = LocalResult::default();
                
                match frame.ms_level {
                    MSLevel::MS1 => {
                        out.ms1 = processor.process_peaks_batch_optimized(
                            &frame.tof_indices,
                            &frame.intensities,
                            &frame.scan_offsets,
                            rt_min,
                            frame.index as u32,
                            None,
                        );
                    }
                    MSLevel::MS2 => {
                        let qs = &frame.quadrupole_settings;
                        for win in 0..qs.isolation_mz.len() {
                            if win >= qs.isolation_width.len() { break; }
                            
                            let prec_mz = qs.isolation_mz[win] as f32;
                            let width = qs.isolation_width[win] as f32;
                            let key = (
                                quantize(prec_mz - width * 0.5),
                                quantize(prec_mz + width * 0.5)
                            );
                            
                            let td = processor.process_peaks_batch_optimized(
                                &frame.tof_indices,
                                &frame.intensities,
                                &frame.scan_offsets,
                                rt_min,
                                frame.index as u32,
                                Some((qs.scan_starts[win], qs.scan_ends[win])),
                            );
                            
                            if !td.mz_values.is_empty() {
                                out.ms2.entry(key)
                                    .or_insert_with(|| TimsTOFData::with_capacity(td.mz_values.len()))
                                    .merge_from(td);
                            }
                        }
                    }
                    _ => {}
                }
                
                processed_clone.fetch_add(1, AtomicOrdering::Relaxed);
                out
            }
        )
        .reduce(
            || LocalResult::default(),
            |mut a, mut b| {
                // Merge MS1 data
                a.ms1.merge_from(b.ms1);
                
                // Merge MS2 data
                for (k, v) in b.ms2 {
                    a.ms2.entry(k)
                        .or_insert_with(|| TimsTOFData::with_capacity(v.mz_values.len()))
                        .merge_from(v);
                }
                a
            }
        );
    
    // Convert MS2 HashMap to Vec
    let ms2_vec: Vec<_> = result.ms2
        .into_iter()
        .map(|((lo, hi), data)| {
            ((lo as f32 / 10_000.0, hi as f32 / 10_000.0), data)
        })
        .collect();
    
    println!("  Frame processing: {:.3}s", process_start.elapsed().as_secs_f32());
    println!("  Frames processed: {}", processed_count.load(AtomicOrdering::Relaxed));
    
    println!("Total loading time: {:.3}s", total_start.elapsed().as_secs_f32());
    
    Ok(TimsTOFRawData {
        ms1_data: result.ms1,
        ms2_windows: ms2_vec,
    })
}

// ============================================================================
// Indexing - Optimized Version
// ============================================================================

impl IndexedTimsTOFData {
    pub fn from_timstof_data(data: TimsTOFData) -> Self {
        let n_peaks = data.mz_values.len();
        
        const PARALLEL_THRESHOLD: usize = 10_000;
        
        if n_peaks < PARALLEL_THRESHOLD {
            Self::from_timstof_data_sequential(data)
        } else {
            Self::from_timstof_data_parallel_optimized(data)
        }
    }
    
    fn from_timstof_data_sequential(data: TimsTOFData) -> Self {
        let n_peaks = data.mz_values.len();
        
        let mut order: Vec<usize> = (0..n_peaks).collect();
        order.sort_unstable_by(|&a, &b| {
            data.mz_values[a].partial_cmp(&data.mz_values[b]).unwrap()
        });

        fn reorder_f32(src: &[f32], ord: &[usize]) -> Vec<f32> {
            ord.iter().map(|&i| src[i]).collect()
        }
        
        fn reorder_u32(src: &[u32], ord: &[usize]) -> Vec<u32> {
            ord.iter().map(|&i| src[i]).collect()
        }

        Self {
            rt_values_min: reorder_f32(&data.rt_values_min, &order),
            mobility_values: reorder_f32(&data.mobility_values, &order),
            mz_values: reorder_f32(&data.mz_values, &order),
            intensity_values: reorder_u32(&data.intensity_values, &order),
            frame_indices: reorder_u32(&data.frame_indices, &order),
            scan_indices: reorder_u32(&data.scan_indices, &order),
        }
    }
    
    fn from_timstof_data_parallel_optimized(data: TimsTOFData) -> Self {
        let n_peaks = data.mz_values.len();
        
        // Generate order indices directly without intermediate tuple
        let mut order: Vec<usize> = (0..n_peaks).collect();
        order.par_sort_unstable_by(|&a, &b| {
            data.mz_values[a].partial_cmp(&data.mz_values[b]).unwrap()
        });
        
        // Parallel reordering of all columns
        let ((rt_values_min, mobility_values), ((mz_values, intensity_values), (frame_indices, scan_indices))) = rayon::join(
            || {
                rayon::join(
                    || Self::reorder_f32_parallel(&data.rt_values_min, &order),
                    || Self::reorder_f32_parallel(&data.mobility_values, &order),
                )
            },
            || {
                rayon::join(
                    || {
                        rayon::join(
                            || Self::reorder_f32_parallel(&data.mz_values, &order),
                            || Self::reorder_u32_parallel(&data.intensity_values, &order),
                        )
                    },
                    || {
                        rayon::join(
                            || Self::reorder_u32_parallel(&data.frame_indices, &order),
                            || Self::reorder_u32_parallel(&data.scan_indices, &order),
                        )
                    }
                )
            }
        );
        
        Self {
            rt_values_min,
            mobility_values,
            mz_values,
            intensity_values,
            frame_indices,
            scan_indices,
        }
    }
    
    #[inline(always)]
    fn reorder_f32_parallel(src: &[f32], order: &[usize]) -> Vec<f32> {
        order.par_iter().map(|&i| src[i]).collect()
    }
    
    #[inline(always)]
    fn reorder_u32_parallel(src: &[u32], order: &[usize]) -> Vec<u32> {
        order.par_iter().map(|&i| src[i]).collect()
    }
}

pub fn build_indexed_data(raw_data: TimsTOFRawData) -> Result<(IndexedTimsTOFData, Vec<((f32, f32), IndexedTimsTOFData)>), Box<dyn Error>> {
    let ms1_size = raw_data.ms1_data.mz_values.len();
    let ms2_count = raw_data.ms2_windows.len();
    
    println!("Building indexed data: MS1 points={}, MS2 windows={}", ms1_size, ms2_count);
    
    let (ms1_indexed, ms2_indexed_pairs) = rayon::join(
        || {
            IndexedTimsTOFData::from_timstof_data(raw_data.ms1_data)
        },
        || {
            if ms2_count > 1 {
                raw_data.ms2_windows
                    .into_par_iter()
                    .map(|((low, high), data)| {
                        let indexed = IndexedTimsTOFData::from_timstof_data(data);
                        ((low, high), indexed)
                    })
                    .collect()
            } else {
                raw_data.ms2_windows
                    .into_iter()
                    .map(|((low, high), data)| {
                        let indexed = IndexedTimsTOFData::from_timstof_data(data);
                        ((low, high), indexed)
                    })
                    .collect()
            }
        }
    );
    
    Ok((ms1_indexed, ms2_indexed_pairs))
}

// ============================================================================
// Helper Functions
// ============================================================================

#[inline(always)]
pub fn quantize(x: f32) -> u32 { 
    (x * 10_000.0).round() as u32 
}

fn parse_args() -> (String, Option<PathBuf>, Option<SaveFormat>) {
    let args: Vec<String> = env::args().collect();
    
    let mut d_folder = String::new();
    let mut output_dir = None;
    let mut save_format = None;
    
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--save" | "-s" => {
                if i + 1 < args.len() {
                    output_dir = Some(PathBuf::from(&args[i + 1]));
                    i += 2;
                } else {
                    eprintln!("Error: --save requires an output directory");
                    std::process::exit(1);
                }
            }
            "--format" | "-f" => {
                if i + 1 < args.len() {
                    save_format = match args[i + 1].to_lowercase().as_str() {
                        "json" => Some(SaveFormat::Json),
                        "bincode" | "bin" => Some(SaveFormat::Bincode),
                        "csv" => Some(SaveFormat::Csv),
                        _ => {
                            eprintln!("Error: Unknown format '{}'. Use json, bincode, or csv", args[i + 1]);
                            std::process::exit(1);
                        }
                    };
                    i += 2;
                } else {
                    eprintln!("Error: --format requires a format type (json, bincode, csv)");
                    std::process::exit(1);
                }
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            _ => {
                if d_folder.is_empty() && !args[i].starts_with('-') {
                    d_folder = args[i].clone();
                }
                i += 1;
            }
        }
    }
    
    // Set default data folder based on OS if not provided
    if d_folder.is_empty() {
        d_folder = if std::env::consts::OS == "macos" {
            "/Users/augustsirius/Desktop/DIA_peak_group_extraction/输入数据文件/raw_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d".to_string()
        } else {
            "/wangshuaiyao/dia-bert-timstof/test_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d".to_string()
        };
    }
    
    // If output_dir is set but format is not, default to bincode
    if output_dir.is_some() && save_format.is_none() {
        save_format = Some(SaveFormat::Bincode);
    }
    
    (d_folder, output_dir, save_format)
}

fn print_help() {
    println!("TIMSTOF Data Loader - Load and optionally save indexed TimsTOF data");
    println!();
    println!("Usage: timstof_data_loader [DATA_FOLDER] [OPTIONS]");
    println!();
    println!("Arguments:");
    println!("  DATA_FOLDER        Path to the .d folder containing TimsTOF data");
    println!();
    println!("Options:");
    println!("  -s, --save DIR     Save indexed data to the specified directory");
    println!("  -f, --format TYPE  Format for saving (json, bincode, csv) [default: bincode]");
    println!("  -h, --help         Display this help message");
    println!();
    println!("Examples:");
    println!("  # Load data without saving");
    println!("  timstof_data_loader /path/to/data.d");
    println!();
    println!("  # Load and save as bincode (fastest, smallest)");
    println!("  timstof_data_loader /path/to/data.d --save ./output");
    println!();
    println!("  # Load and save as JSON (human-readable)");
    println!("  timstof_data_loader /path/to/data.d --save ./output --format json");
    println!();
    println!("  # Load and save as CSV (for analysis in other tools)");
    println!("  timstof_data_loader /path/to/data.d --save ./output --format csv");
}

// ============================================================================
// Main
// ============================================================================

fn main() -> Result<(), Box<dyn Error>> {
    // Initialize thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .build_global()
        .unwrap();
    
    let (d_folder, output_dir, save_format) = parse_args();
    let d_path = Path::new(&d_folder);
    
    if !d_path.exists() {
        return Err(format!("Folder {:?} not found", d_path).into());
    }
    
    // Extract base name from the data folder
    let base_name = d_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("timstof_data")
        .to_string();
    
    println!("\n========== TIMSTOF DATA LOADER (OPTIMIZED) ==========");
    println!("Data folder: {}", d_folder);
    println!("Threads: {}", NUM_THREADS);
    if let Some(ref dir) = output_dir {
        println!("Output directory: {:?}", dir);
        println!("Save format: {:?}", save_format.unwrap());
    }
    
    // Step 1: Read raw data
    println!("\n[Step 1] Loading raw data...");
    let load_start = Instant::now();
    let raw_data = read_timstof_data(d_path)?;
    let load_time = load_start.elapsed().as_secs_f32();
    
    println!("\n[Step 1 Summary]");
    println!("  MS1 data points: {}", raw_data.ms1_data.mz_values.len());
    println!("  MS2 windows: {}", raw_data.ms2_windows.len());
    let total_ms2: usize = raw_data.ms2_windows.iter().map(|(_, d)| d.mz_values.len()).sum();
    println!("  MS2 data points: {}", total_ms2);
    println!("  Loading time: {:.3}s", load_time);
    
    // Step 2: Build index
    println!("\n[Step 2] Building index...");
    let index_start = Instant::now();
    let (ms1_indexed, ms2_indexed_pairs) = build_indexed_data(raw_data)?;
    let index_time = index_start.elapsed().as_secs_f32();
    
    println!("\n[Step 2 Summary]");
    println!("  MS1 indexed points: {}", ms1_indexed.mz_values.len());
    println!("  MS2 indexed windows: {}", ms2_indexed_pairs.len());
    println!("  Indexing time: {:.3}s", index_time);
    
    // Step 3: Save data if requested
    if let Some(output_dir) = output_dir {
        println!("\n[Step 3] Saving indexed data...");
        let saver = DataSaver::new(output_dir.clone(), save_format.unwrap())?;
        saver.save_indexed_data(&ms1_indexed, &ms2_indexed_pairs, &base_name)?;
        
        println!("\n[Step 3 Summary]");
        println!("  Data successfully saved to: {:?}", output_dir);
        
    //     // Optionally demonstrate loading the saved data
    //     if save_format != Some(SaveFormat::Csv) {
    //         println!("\n[Verification] Testing data load...");
    //         let load_test_start = Instant::now();
    //         let (loaded_ms1, loaded_ms2) = load_indexed_data(&output_dir, &base_name, save_format.unwrap())?;
    //         println!("  Loaded MS1 points: {}", loaded_ms1.mz_values.len());
    //         println!("  Loaded MS2 windows: {}", loaded_ms2.len());
    //         println!("  Load time: {:.3}s", load_test_start.elapsed().as_secs_f32());
    //     }
    // }
    
    // Final summary
    println!("\n========== COMPLETE ==========");
    println!("Total time: {:.3}s", load_time + index_time);
    println!("Data successfully loaded and indexed!");
    
    Ok(())
}