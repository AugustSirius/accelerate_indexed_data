use std::{error::Error, path::Path, time::Instant};
use std::collections::HashMap;
use rayon::prelude::*;
use timsrust::{converters::ConvertableDomain, readers::{FrameReader, MetadataReader}, MSLevel};
use std::sync::Arc;

// ============================================================================
// Data Structures
// ============================================================================

#[derive(Debug, Clone)]
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
        Self {
            rt_values_min: Vec::with_capacity(capacity),
            mobility_values: Vec::with_capacity(capacity),
            mz_values: Vec::with_capacity(capacity),
            intensity_values: Vec::with_capacity(capacity),
            frame_indices: Vec::with_capacity(capacity),
            scan_indices: Vec::with_capacity(capacity),
        }
    }
    
    pub fn reserve(&mut self, additional: usize) {
        self.rt_values_min.reserve(additional);
        self.mobility_values.reserve(additional);
        self.mz_values.reserve(additional);
        self.intensity_values.reserve(additional);
        self.frame_indices.reserve(additional);
        self.scan_indices.reserve(additional);
    }
}

#[derive(Debug, Clone)]
pub struct TimsTOFRawData {
    pub ms1_data: TimsTOFData,
    pub ms2_windows: Vec<((f32, f32), TimsTOFData)>,
}

struct FrameSplit {
    pub ms1: TimsTOFData,
    pub ms2: Vec<((u32, u32), TimsTOFData)>,
}

trait MergeFrom {
    fn merge_from(&mut self, other: &mut Self);
}

impl MergeFrom for TimsTOFData {
    fn merge_from(&mut self, other: &mut Self) {
        self.rt_values_min.append(&mut other.rt_values_min);
        self.mobility_values.append(&mut other.mobility_values);
        self.mz_values.append(&mut other.mz_values);
        self.intensity_values.append(&mut other.intensity_values);
        self.frame_indices.append(&mut other.frame_indices);
        self.scan_indices.append(&mut other.scan_indices);
    }
}

// ============================================================================
// Helper Functions - OPTIMIZED
// ============================================================================

#[inline]
fn quantize(x: f32) -> u32 {
    (x * 10_000.0).round() as u32
}

// CRITICAL OPTIMIZATION: Build scan lookup table for O(1) access
#[inline]
fn build_scan_lookup(scan_offsets: &[usize]) -> Vec<u32> {
    if scan_offsets.is_empty() {
        return Vec::new();
    }
    
    let max_index = scan_offsets.last().copied().unwrap_or(0);
    let mut lookup = vec![0u32; max_index];
    
    for (scan, window) in scan_offsets.windows(2).enumerate() {
        let start = window[0];
        let end = window[1];
        for idx in start..end {
            lookup[idx] = scan as u32;
        }
    }
    
    // Handle last scan
    if scan_offsets.len() > 1 {
        let last_scan = (scan_offsets.len() - 2) as u32;
        for idx in scan_offsets[scan_offsets.len() - 1]..max_index {
            lookup[idx] = last_scan;
        }
    }
    
    lookup
}

// ============================================================================
// Core Data Reading Function - FULLY OPTIMIZED
// ============================================================================

fn read_timstof_data(d_folder: &Path) -> Result<TimsTOFRawData, Box<dyn Error>> {
    let start_time = Instant::now();
    println!("Reading TimsTOF data from: {:?}", d_folder);
    
    // Step 1: Read metadata
    let metadata_start = Instant::now();
    let tdf_path = d_folder.join("analysis.tdf");
    let meta = MetadataReader::new(&tdf_path)?;
    let mz_cv = Arc::new(meta.mz_converter);
    let im_cv = Arc::new(meta.im_converter);
    println!("  ✓ Metadata read: {:.2} ms", metadata_start.elapsed().as_secs_f32() * 1000.0);
    
    // Step 2: Initialize frame reader
    let frame_init_start = Instant::now();
    let frames = FrameReader::new(d_folder)?;
    let n_frames = frames.len();
    println!("  ✓ Frame reader initialized: {:.2} ms ({} frames)", 
             frame_init_start.elapsed().as_secs_f32() * 1000.0, n_frames);
    
    // Step 3: Process frames in parallel with optimizations
    let frame_proc_start = Instant::now();
    println!("  Processing {} frames in parallel...", n_frames);
    
    let splits: Vec<FrameSplit> = (0..n_frames).into_par_iter().map(|idx| {
        let frame = frames.get(idx).expect("frame read");
        let rt_min = frame.rt_in_seconds as f32 / 60.0;
        let mut ms1 = TimsTOFData::new();
        let mut ms2_pairs: Vec<((u32,u32), TimsTOFData)> = Vec::new();
        
        match frame.ms_level {
            MSLevel::MS1 => {
                let n_peaks = frame.tof_indices.len();
                ms1 = TimsTOFData::with_capacity(n_peaks);
                
                // Build scan lookup table once per frame
                let scan_lookup = build_scan_lookup(&frame.scan_offsets);
                
                for (p_idx, (&tof, &intensity)) in frame.tof_indices.iter()
                    .zip(frame.intensities.iter())
                    .enumerate() 
                {
                    let mz = mz_cv.convert(tof as f64) as f32;
                    
                    // O(1) lookup instead of O(n) search
                    let scan = if p_idx < scan_lookup.len() {
                        scan_lookup[p_idx]
                    } else {
                        (frame.scan_offsets.len() - 1) as u32
                    };
                    
                    let im = im_cv.convert(scan as f64) as f32;
                    
                    ms1.rt_values_min.push(rt_min);
                    ms1.mobility_values.push(im);
                    ms1.mz_values.push(mz);
                    ms1.intensity_values.push(intensity);
                    ms1.frame_indices.push(frame.index as u32);
                    ms1.scan_indices.push(scan);
                }
            }
            MSLevel::MS2 => {
                let qs = &frame.quadrupole_settings;
                ms2_pairs.reserve(qs.isolation_mz.len());
                
                // Build scan lookup table once for MS2 frame
                let scan_lookup = build_scan_lookup(&frame.scan_offsets);
                
                for win in 0..qs.isolation_mz.len() {
                    if win >= qs.isolation_width.len() { break; }
                    
                    let prec_mz = qs.isolation_mz[win] as f32;
                    let width = qs.isolation_width[win] as f32;
                    let low = prec_mz - width * 0.5;
                    let high = prec_mz + width * 0.5;
                    let key = (quantize(low), quantize(high));
                    
                    let win_start = qs.scan_starts[win];
                    let win_end = qs.scan_ends[win];
                    
                    // Estimate capacity
                    let estimated_peaks = frame.tof_indices.len() / qs.isolation_mz.len();
                    let mut td = TimsTOFData::with_capacity(estimated_peaks);
                    
                    for (p_idx, (&tof, &intensity)) in frame.tof_indices.iter()
                        .zip(frame.intensities.iter())
                        .enumerate() 
                    {
                        // O(1) lookup
                        let scan = if p_idx < scan_lookup.len() {
                            scan_lookup[p_idx]
                        } else {
                            (frame.scan_offsets.len() - 1) as u32
                        };
                        
                        if scan < win_start as u32 || scan > win_end as u32 { 
                            continue; 
                        }
                        
                        let mz = mz_cv.convert(tof as f64) as f32;
                        let im = im_cv.convert(scan as f64) as f32;
                        
                        td.rt_values_min.push(rt_min);
                        td.mobility_values.push(im);
                        td.mz_values.push(mz);
                        td.intensity_values.push(intensity);
                        td.frame_indices.push(frame.index as u32);
                        td.scan_indices.push(scan);
                    }
                    
                    if !td.mz_values.is_empty() {
                        ms2_pairs.push((key, td));
                    }
                }
            }
            _ => {}
        }
        FrameSplit { ms1, ms2: ms2_pairs }
    }).collect();
    
    println!("  ✓ Frame processing: {:.2} ms", frame_proc_start.elapsed().as_secs_f32() * 1000.0);
    
    // Step 4: Merge MS1 data - OPTIMIZED
    let ms1_merge_start = Instant::now();
    println!("  Merging MS1 data...");
    
    // Calculate exact size for single allocation
    let ms1_total_size: usize = splits.par_iter()
        .map(|s| s.ms1.mz_values.len())
        .sum();
    
    let mut global_ms1 = TimsTOFData::with_capacity(ms1_total_size);
    
    for split in &splits {
        if !split.ms1.mz_values.is_empty() {
            global_ms1.rt_values_min.extend(&split.ms1.rt_values_min);
            global_ms1.mobility_values.extend(&split.ms1.mobility_values);
            global_ms1.mz_values.extend(&split.ms1.mz_values);
            global_ms1.intensity_values.extend(&split.ms1.intensity_values);
            global_ms1.frame_indices.extend(&split.ms1.frame_indices);
            global_ms1.scan_indices.extend(&split.ms1.scan_indices);
        }
    }
    println!("  ✓ MS1 merge: {:.2} ms ({} peaks)", 
             ms1_merge_start.elapsed().as_secs_f32() * 1000.0, 
             global_ms1.mz_values.len());
    
    // Step 5: Merge MS2 data - NEW OPTIMIZED APPROACH
    let ms2_merge_start = Instant::now();
    println!("  Merging MS2 data...");
    
    // NEW APPROACH: First collect all MS2 data without merging
    let all_ms2_data: Vec<Vec<((u32, u32), TimsTOFData)>> = splits
        .into_iter()
        .map(|split| split.ms2)
        .collect();
    
    // Group by key efficiently
    let mut ms2_grouped: HashMap<(u32, u32), Vec<TimsTOFData>> = HashMap::with_capacity(64);
    
    // Collect all data for each window
    for frame_ms2 in all_ms2_data {
        for (key, data) in frame_ms2 {
            if !data.mz_values.is_empty() {
                ms2_grouped.entry(key)
                    .or_insert_with(Vec::new)
                    .push(data);
            }
        }
    }
    
    // Now merge each window's data once with exact pre-allocation
    let mut ms2_hash: HashMap<(u32, u32), TimsTOFData> = HashMap::with_capacity(ms2_grouped.len());
    
    for (key, data_vec) in ms2_grouped {
        if data_vec.is_empty() { continue; }
        
        // Calculate total size for this window
        let total_size: usize = data_vec.iter()
            .map(|d| d.mz_values.len())
            .sum();
        
        // Create merged data with exact capacity
        let mut merged = TimsTOFData::with_capacity(total_size);
        
        // Merge all data for this window
        for mut data in data_vec {
            merged.rt_values_min.append(&mut data.rt_values_min);
            merged.mobility_values.append(&mut data.mobility_values);
            merged.mz_values.append(&mut data.mz_values);
            merged.intensity_values.append(&mut data.intensity_values);
            merged.frame_indices.append(&mut data.frame_indices);
            merged.scan_indices.append(&mut data.scan_indices);
        }
        
        ms2_hash.insert(key, merged);
    }
    
    println!("  ✓ MS2 merge: {:.2} ms ({} windows)", 
             ms2_merge_start.elapsed().as_secs_f32() * 1000.0, 
             ms2_hash.len());
    
    // Step 6: Convert to final format
    let ms2_convert_start = Instant::now();
    let mut ms2_vec = Vec::with_capacity(ms2_hash.len());
    
    for ((q_low, q_high), td) in ms2_hash {
        let low = q_low as f32 / 10_000.0;
        let high = q_high as f32 / 10_000.0;
        ms2_vec.push(((low, high), td));
    }
    
    // Sort by window for consistent output
    ms2_vec.sort_by(|a, b| a.0.0.partial_cmp(&b.0.0).unwrap());
    
    println!("  ✓ MS2 convert: {:.2} ms", ms2_convert_start.elapsed().as_secs_f32() * 1000.0);
    
    println!("  MS1 data points: {}", global_ms1.mz_values.len());
    println!("  MS2 windows: {}", ms2_vec.len());
    
    let total_ms2_peaks: usize = ms2_vec.iter().map(|(_, td)| td.mz_values.len()).sum();
    println!("  MS2 data points: {}", total_ms2_peaks);
    
    println!("\n  Total processing time: {:.2} seconds", start_time.elapsed().as_secs_f32());
    
    Ok(TimsTOFRawData {
        ms1_data: global_ms1,
        ms2_windows: ms2_vec,
    })
}

// ============================================================================
// Main Function
// ============================================================================

fn main() -> Result<(), Box<dyn Error>> {
    // AUTOMATIC CORE DETECTION - Uses all available cores
    // Comment out these lines if you want to manually set thread count
    let num_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    
    // Optional: Set a maximum to prevent over-subscription
    // let num_threads = num_threads.min(64);  // Cap at 64 threads max
    
    // Optional: Use a specific number of threads
    // let num_threads = 32;  // Uncomment to force 32 threads
    
    // Configure thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
        .unwrap();
    
    // Hard-coded path to TimsTOF data - CHANGE THIS TO YOUR PATH
    // let data_path = "/path/to/your/data.d";
    // let data_path = "/Users/augustsirius/Desktop/DIA_peak_group_extraction/输入数据文件/raw_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d";
    let data_path = "/wangshuaiyao/dia-bert-timstof/test_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d";
    
    let d_path = Path::new(data_path);
    
    println!("\n========== Fully Optimized TimsTOF Raw Data Reader ==========");
    println!("Data folder: {}", data_path);
    println!("Available CPU cores: {}", num_threads);
    println!("Thread pool size: {}", rayon::current_num_threads());
    
    let start_time = Instant::now();
    
    // Read raw data with all optimizations
    let raw_data = read_timstof_data(d_path)?;
    
    println!("\n========== Reading Complete ==========");
    println!("Total time: {:.2} seconds", start_time.elapsed().as_secs_f32());
    println!("MS1 data points: {}", raw_data.ms1_data.mz_values.len());
    println!("MS2 windows: {}", raw_data.ms2_windows.len());
    
    // Calculate and display performance metrics
    let total_peaks = raw_data.ms1_data.mz_values.len() + 
        raw_data.ms2_windows.iter().map(|(_, td)| td.mz_values.len()).sum::<usize>();
    let elapsed_secs = start_time.elapsed().as_secs_f32();
    
    println!("\n========== Performance Metrics ==========");
    println!("Total peaks processed: {}", total_peaks);
    println!("Throughput: {:.0} peaks/second", total_peaks as f32 / elapsed_secs);
    println!("Average time per frame: {:.2} ms", elapsed_secs * 1000.0 / 53168.0);
    
    // Memory estimate
    let memory_bytes = total_peaks * 6 * 4;  // 6 arrays × 4 bytes each
    let memory_gb = memory_bytes as f32 / 1024.0 / 1024.0 / 1024.0;
    println!("Estimated memory usage: {:.2} GB", memory_gb);
    
    Ok(())
}