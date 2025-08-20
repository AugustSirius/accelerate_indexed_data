use std::{error::Error, path::Path, time::Instant};
use std::collections::HashMap;
use rayon::prelude::*;
use timsrust::{converters::ConvertableDomain, readers::{FrameReader, MetadataReader}, MSLevel, Frame};
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
// Helper Functions
// ============================================================================

#[inline(always)]
fn quantize(x: f32) -> u32 {
    (x * 10_000.0).round() as u32
}

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
    
    if scan_offsets.len() > 1 {
        let last_scan = (scan_offsets.len() - 2) as u32;
        for idx in scan_offsets[scan_offsets.len() - 1]..max_index {
            lookup[idx] = last_scan;
        }
    }
    
    lookup
}

// Process a single frame
fn process_frame(
    frame: Frame,
    mz_cv: &Arc<timsrust::converters::Tof2MzConverter>,
    im_cv: &Arc<timsrust::converters::Scan2ImConverter>,
) -> FrameSplit {
    let rt_min = frame.rt_in_seconds as f32 / 60.0;
    let mut ms1 = TimsTOFData::new();
    let mut ms2_pairs: Vec<((u32,u32), TimsTOFData)> = Vec::new();
    
    match frame.ms_level {
        MSLevel::MS1 => {
            let n_peaks = frame.tof_indices.len();
            ms1 = TimsTOFData::with_capacity(n_peaks);
            
            let scan_lookup = build_scan_lookup(&frame.scan_offsets);
            
            for (p_idx, (&tof, &intensity)) in frame.tof_indices.iter()
                .zip(frame.intensities.iter())
                .enumerate() 
            {
                let mz = mz_cv.convert(tof as f64) as f32;
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
                
                let estimated_peaks = frame.tof_indices.len() / qs.isolation_mz.len().max(1);
                let mut td = TimsTOFData::with_capacity(estimated_peaks);
                
                for (p_idx, (&tof, &intensity)) in frame.tof_indices.iter()
                    .zip(frame.intensities.iter())
                    .enumerate() 
                {
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
}

// ============================================================================
// HPC-OPTIMIZED: Two-Phase Processing
// ============================================================================

fn read_timstof_data_hpc(d_folder: &Path, worker_threads: usize) -> Result<TimsTOFRawData, Box<dyn Error>> {
    let start_time = Instant::now();
    println!("Reading TimsTOF data from: {:?}", d_folder);
    println!("Strategy: Two-phase processing with {} worker threads", worker_threads);
    
    // Step 1: Read metadata
    let metadata_start = Instant::now();
    let tdf_path = d_folder.join("analysis.tdf");
    let meta = MetadataReader::new(&tdf_path)?;
    let mz_cv = Arc::new(meta.mz_converter);
    let im_cv = Arc::new(meta.im_converter);
    println!("  ✓ Metadata read: {:.2} ms", metadata_start.elapsed().as_secs_f32() * 1000.0);
    
    // Step 2: Initialize frame reader
    let frame_init_start = Instant::now();
    let frames = Arc::new(FrameReader::new(d_folder)?);
    let n_frames = frames.len();
    println!("  ✓ Frame reader initialized: {:.2} ms ({} frames)", 
             frame_init_start.elapsed().as_secs_f32() * 1000.0, n_frames);
    
    // ============================================================================
    // PHASE 1: Sequential Frame Reading (Optimize for Network I/O)
    // ============================================================================
    println!("\n[Phase 1: Frame Reading - Sequential for Network I/O]");
    let frame_read_start = Instant::now();
    
    // Read all frames sequentially to optimize network I/O
    // This is counter-intuitive but often faster on HPC with network storage
    let mut all_frames = Vec::with_capacity(n_frames);
    
    // Use larger batches for progress reporting
    let progress_interval = 5000;
    for idx in 0..n_frames {
        all_frames.push(frames.get(idx).expect("frame read"));
        
        if (idx + 1) % progress_interval == 0 {
            print!("\r  Progress: {}/{} frames ({:.1}%)", 
                   idx + 1, n_frames, (idx + 1) as f32 / n_frames as f32 * 100.0);
            use std::io::{self, Write};
            io::stdout().flush().unwrap();
        }
    }
    println!("\r  ✓ All frames loaded: {:.2} seconds", frame_read_start.elapsed().as_secs_f32());
    
    // ============================================================================
    // PHASE 2: Parallel Processing (Pure CPU work, no I/O)
    // ============================================================================
    println!("\n[Phase 2: Frame Processing - {} threads, no I/O contention]", worker_threads);
    let frame_proc_start = Instant::now();
    
    // Now process with full parallelism - no I/O contention!
    let splits: Vec<FrameSplit> = all_frames
        .into_par_iter()
        .map(|frame| process_frame(frame, &mz_cv, &im_cv))
        .collect();
    
    println!("  ✓ Frame processing: {:.2} seconds", frame_proc_start.elapsed().as_secs_f32());
    
    // ============================================================================
    // Rest remains the same - proven to work
    // ============================================================================
    
    // Merge MS1 data
    let ms1_merge_start = Instant::now();
    println!("\n[Phase 3: MS1 Merge]");
    
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
    println!("  ✓ MS1 merge: {:.2} seconds ({} peaks)", 
             ms1_merge_start.elapsed().as_secs_f32(), global_ms1.mz_values.len());
    
    // Merge MS2 data
    let ms2_merge_start = Instant::now();
    println!("\n[Phase 4: MS2 Merge]");
    
    let mut ms2_hash: HashMap<(u32,u32), TimsTOFData> = HashMap::with_capacity(64);
    
    let mut window_sizes: HashMap<(u32, u32), usize> = HashMap::with_capacity(64);
    for split in &splits {
        for (key, td) in &split.ms2 {
            *window_sizes.entry(*key).or_insert(0) += td.mz_values.len();
        }
    }
    
    for (key, size) in window_sizes {
        ms2_hash.insert(key, TimsTOFData::with_capacity(size));
    }
    
    for mut split in splits {
        for (key, mut td) in split.ms2 {
            if let Some(entry) = ms2_hash.get_mut(&key) {
                entry.merge_from(&mut td);
            }
        }
    }
    
    let mut ms2_vec = Vec::with_capacity(ms2_hash.len());
    for ((q_low, q_high), td) in ms2_hash {
        let low = q_low as f32 / 10_000.0;
        let high = q_high as f32 / 10_000.0;
        ms2_vec.push(((low, high), td));
    }
    
    ms2_vec.sort_by(|a, b| a.0.0.partial_cmp(&b.0.0).unwrap());
    
    println!("  ✓ MS2 merge: {:.2} seconds ({} windows)", 
             ms2_merge_start.elapsed().as_secs_f32(), ms2_vec.len());
    
    let total_ms2_peaks: usize = ms2_vec.iter().map(|(_, td)| td.mz_values.len()).sum();
    
    println!("\n════════════════════════════════════════");
    println!("Total processing time: {:.2} seconds", start_time.elapsed().as_secs_f32());
    println!("MS1 peaks: {}", global_ms1.mz_values.len());
    println!("MS2 windows: {}", ms2_vec.len());
    println!("MS2 peaks: {}", total_ms2_peaks);
    
    Ok(TimsTOFRawData {
        ms1_data: global_ms1,
        ms2_windows: ms2_vec,
    })
}

// ============================================================================
// Platform Detection and Adaptive Configuration
// ============================================================================

fn detect_optimal_threads() -> usize {
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    
    // Check if we're on HPC (usually 32+ cores) or workstation
    if cpu_count >= 32 {
        // HPC: Use 50% of cores to leave room for I/O and system
        println!("Detected HPC environment ({} cores)", cpu_count);
        (cpu_count / 2).max(32).min(48)
    } else {
        // Workstation/Mac: Can use more threads than cores due to better scheduling
        println!("Detected workstation environment ({} cores)", cpu_count);
        // Use 2-4x threads for hyperthreading benefit
        (cpu_count * 2).min(64)
    }
}

// ============================================================================
// Main Function
// ============================================================================

fn main() -> Result<(), Box<dyn Error>> {
    let available_cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    
    // Adaptive thread configuration
    let optimal_threads = if std::env::var("FORCE_THREADS").is_ok() {
        std::env::var("FORCE_THREADS")
            .unwrap()
            .parse::<usize>()
            .unwrap_or(64)
    } else {
        detect_optimal_threads()
    };
    
    // Configure thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(optimal_threads)
        .thread_name(|i| format!("worker-{:02}", i))
        .build_global()
        .unwrap();
    
    // Data path - adjust based on platform
    #[cfg(target_os = "macos")]
    let data_path = "/Users/augustsirius/Desktop/DIA_peak_group_extraction/输入数据文件/raw_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d";
    
    #[cfg(not(target_os = "macos"))]
    let data_path = "/wangshuaiyao/dia-bert-timstof/test_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d";
    
    let d_path = Path::new(data_path);
    if !d_path.exists() {
        return Err(format!("Data folder {:?} not found", d_path).into());
    }
    
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║          HPC-OPTIMIZED TimsTOF Raw Data Reader                ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("Platform: {}", std::env::consts::OS);
    println!("Available cores: {}", available_cores);
    println!("Worker threads: {}", optimal_threads);
    println!("Data folder: {}", data_path);
    println!("\nTip: Set FORCE_THREADS=32 to override thread detection");
    
    let start_time = Instant::now();
    
    // Use HPC-optimized version
    let raw_data = read_timstof_data_hpc(d_path, optimal_threads)?;
    
    let elapsed = start_time.elapsed();
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║                     PROCESSING COMPLETE                       ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("Total time: {:.2} seconds", elapsed.as_secs_f32());
    
    let total_ms2: usize = raw_data.ms2_windows.iter()
        .map(|(_, td)| td.mz_values.len())
        .sum();
    let total_peaks = raw_data.ms1_data.mz_values.len() + total_ms2;
    println!("Total peaks: {}", total_peaks);
    println!("Throughput: {:.0} peaks/second", total_peaks as f32 / elapsed.as_secs_f32());
    
    // Verify consistency
    if raw_data.ms1_data.mz_values.len() == 591487824 
        && raw_data.ms2_windows.len() == 32 
        && total_ms2 == 2034948969 {
        println!("✓ Output verification PASSED!");
    } else {
        println!("✗ Output verification FAILED!");
    }
    
    Ok(())
}