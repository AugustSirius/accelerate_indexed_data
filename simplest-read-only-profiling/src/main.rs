use std::{error::Error, path::Path, time::Instant};
use std::collections::HashMap;
use rayon::prelude::*;
use timsrust::{converters::ConvertableDomain, readers::{FrameReader, MetadataReader}, MSLevel};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// ============================================================================
// Profiling Structure
// ============================================================================

#[derive(Default)]
struct ProfilingStats {
    // Frame-level stats
    ms1_frames: usize,
    ms2_frames: usize,
    ms1_peaks: usize,
    ms2_peaks: usize,
    
    // Timing breakdown
    metadata_read_ms: f32,
    frame_reader_init_ms: f32,
    frame_processing_ms: f32,
    ms1_merge_ms: f32,
    ms2_merge_ms: f32,
    ms2_convert_ms: f32,
    total_ms: f32,
    
    // Detailed frame processing
    frame_read_ms: f32,
    tof_conversion_ms: f32,
    scan_lookup_ms: f32,
    im_conversion_ms: f32,
    ms1_processing_ms: f32,
    ms2_processing_ms: f32,
    ms2_window_calc_ms: f32,
    
    // Memory stats
    estimated_memory_mb: f32,
}

impl ProfilingStats {
    fn print_report(&self) {
        println!("\n╔══════════════════════════════════════════════════════════════╗");
        println!("║                   PERFORMANCE PROFILING REPORT                ║");
        println!("╠══════════════════════════════════════════════════════════════╣");
        
        println!("║ DATA STATISTICS:                                              ║");
        println!("║   MS1 Frames: {:>8} | MS1 Peaks: {:>15}       ║", self.ms1_frames, self.ms1_peaks);
        println!("║   MS2 Frames: {:>8} | MS2 Peaks: {:>15}       ║", self.ms2_frames, self.ms2_peaks);
        println!("║   Estimated Memory: {:.2} MB                                 ║", self.estimated_memory_mb);
        
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!("║ MAIN PIPELINE TIMING:                                         ║");
        println!("║   1. Metadata Read:        {:>8.2} ms ({:>5.1}%)           ║", 
            self.metadata_read_ms, self.metadata_read_ms / self.total_ms * 100.0);
        println!("║   2. Frame Reader Init:    {:>8.2} ms ({:>5.1}%)           ║", 
            self.frame_reader_init_ms, self.frame_reader_init_ms / self.total_ms * 100.0);
        println!("║   3. Frame Processing:     {:>8.2} ms ({:>5.1}%)           ║", 
            self.frame_processing_ms, self.frame_processing_ms / self.total_ms * 100.0);
        println!("║   4. MS1 Merge:            {:>8.2} ms ({:>5.1}%)           ║", 
            self.ms1_merge_ms, self.ms1_merge_ms / self.total_ms * 100.0);
        println!("║   5. MS2 Merge:            {:>8.2} ms ({:>5.1}%)           ║", 
            self.ms2_merge_ms, self.ms2_merge_ms / self.total_ms * 100.0);
        println!("║   6. MS2 Convert:          {:>8.2} ms ({:>5.1}%)           ║", 
            self.ms2_convert_ms, self.ms2_convert_ms / self.total_ms * 100.0);
        
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!("║ FRAME PROCESSING BREAKDOWN:                                   ║");
        println!("║   Frame Reading:           {:>8.2} ms                      ║", self.frame_read_ms);
        println!("║   TOF→m/z Conversion:      {:>8.2} ms                      ║", self.tof_conversion_ms);
        println!("║   Scan Index Lookup:       {:>8.2} ms                      ║", self.scan_lookup_ms);
        println!("║   Scan→IM Conversion:      {:>8.2} ms                      ║", self.im_conversion_ms);
        println!("║   MS1 Processing:          {:>8.2} ms                      ║", self.ms1_processing_ms);
        println!("║   MS2 Processing:          {:>8.2} ms                      ║", self.ms2_processing_ms);
        println!("║   MS2 Window Calc:         {:>8.2} ms                      ║", self.ms2_window_calc_ms);
        
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!("║ TOTAL TIME: {:>8.2} ms                                      ║", self.total_ms);
        println!("╚══════════════════════════════════════════════════════════════╝");
        
        // Performance metrics
        let peaks_per_sec = (self.ms1_peaks + self.ms2_peaks) as f32 / (self.total_ms / 1000.0);
        let frames_per_sec = (self.ms1_frames + self.ms2_frames) as f32 / (self.total_ms / 1000.0);
        println!("\nPerformance Metrics:");
        println!("  • Peaks/second:  {:.0}", peaks_per_sec);
        println!("  • Frames/second: {:.0}", frames_per_sec);
        println!("  • MB/second:     {:.2}", self.estimated_memory_mb / (self.total_ms / 1000.0));
    }
}

// Global stats for accumulating timing from parallel operations
static FRAME_READ_TIME: AtomicUsize = AtomicUsize::new(0);
static TOF_CONV_TIME: AtomicUsize = AtomicUsize::new(0);
static SCAN_LOOKUP_TIME: AtomicUsize = AtomicUsize::new(0);
static IM_CONV_TIME: AtomicUsize = AtomicUsize::new(0);
static MS1_PROC_TIME: AtomicUsize = AtomicUsize::new(0);
static MS2_PROC_TIME: AtomicUsize = AtomicUsize::new(0);
static MS2_WINDOW_TIME: AtomicUsize = AtomicUsize::new(0);

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
    
    fn estimated_memory_bytes(&self) -> usize {
        self.rt_values_min.len() * 4 +      // f32
        self.mobility_values.len() * 4 +    // f32
        self.mz_values.len() * 4 +          // f32
        self.intensity_values.len() * 4 +   // u32
        self.frame_indices.len() * 4 +      // u32
        self.scan_indices.len() * 4         // u32
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

#[inline]
fn quantize(x: f32) -> u32 {
    (x * 10_000.0).round() as u32
}

fn find_scan_for_index(index: usize, scan_offsets: &[usize]) -> usize {
    for (scan, window) in scan_offsets.windows(2).enumerate() {
        if index >= window[0] && index < window[1] {
            return scan;
        }
    }
    scan_offsets.len() - 1
}

// ============================================================================
// Core Data Reading Function with Detailed Profiling
// ============================================================================

fn read_timstof_data_profiled(d_folder: &Path, stats: &mut ProfilingStats) -> Result<TimsTOFRawData, Box<dyn Error>> {
    println!("Reading TimsTOF data from: {:?}", d_folder);
    
    // Step 1: Read metadata
    let metadata_start = Instant::now();
    let tdf_path = d_folder.join("analysis.tdf");
    let meta = MetadataReader::new(&tdf_path)?;
    let mz_cv = Arc::new(meta.mz_converter);
    let im_cv = Arc::new(meta.im_converter);
    stats.metadata_read_ms = metadata_start.elapsed().as_secs_f32() * 1000.0;
    println!("  ✓ Metadata read: {:.2} ms", stats.metadata_read_ms);
    
    // Step 2: Initialize frame reader
    let frame_init_start = Instant::now();
    let frames = FrameReader::new(d_folder)?;
    let n_frames = frames.len();
    stats.frame_reader_init_ms = frame_init_start.elapsed().as_secs_f32() * 1000.0;
    println!("  ✓ Frame reader initialized: {:.2} ms ({} frames)", stats.frame_reader_init_ms, n_frames);
    
    // Reset atomic counters
    FRAME_READ_TIME.store(0, Ordering::Relaxed);
    TOF_CONV_TIME.store(0, Ordering::Relaxed);
    SCAN_LOOKUP_TIME.store(0, Ordering::Relaxed);
    IM_CONV_TIME.store(0, Ordering::Relaxed);
    MS1_PROC_TIME.store(0, Ordering::Relaxed);
    MS2_PROC_TIME.store(0, Ordering::Relaxed);
    MS2_WINDOW_TIME.store(0, Ordering::Relaxed);
    
    // Step 3: Process frames in parallel
    let frame_proc_start = Instant::now();
    let splits: Vec<FrameSplit> = (0..n_frames).into_par_iter().map(|idx| {
        // Time frame reading
        let frame_read_start = Instant::now();
        let frame = frames.get(idx).expect("frame read");
        let frame_read_us = frame_read_start.elapsed().as_micros() as usize;
        FRAME_READ_TIME.fetch_add(frame_read_us, Ordering::Relaxed);
        
        let rt_min = frame.rt_in_seconds as f32 / 60.0;
        let mut ms1 = TimsTOFData::new();
        let mut ms2_pairs: Vec<((u32,u32), TimsTOFData)> = Vec::new();
        
        match frame.ms_level {
            MSLevel::MS1 => {
                let ms1_start = Instant::now();
                let n_peaks = frame.tof_indices.len();
                ms1 = TimsTOFData::with_capacity(n_peaks);
                
                for (p_idx, (&tof, &intensity)) in frame.tof_indices.iter().zip(frame.intensities.iter()).enumerate() {
                    // Time TOF conversion
                    let tof_start = Instant::now();
                    let mz = mz_cv.convert(tof as f64) as f32;
                    TOF_CONV_TIME.fetch_add(tof_start.elapsed().as_nanos() as usize, Ordering::Relaxed);
                    
                    // Time scan lookup
                    let scan_start = Instant::now();
                    let scan = find_scan_for_index(p_idx, &frame.scan_offsets);
                    SCAN_LOOKUP_TIME.fetch_add(scan_start.elapsed().as_nanos() as usize, Ordering::Relaxed);
                    
                    // Time IM conversion
                    let im_start = Instant::now();
                    let im = im_cv.convert(scan as f64) as f32;
                    IM_CONV_TIME.fetch_add(im_start.elapsed().as_nanos() as usize, Ordering::Relaxed);
                    
                    ms1.rt_values_min.push(rt_min);
                    ms1.mobility_values.push(im);
                    ms1.mz_values.push(mz);
                    ms1.intensity_values.push(intensity);
                    ms1.frame_indices.push(frame.index as u32);
                    ms1.scan_indices.push(scan as u32);
                }
                MS1_PROC_TIME.fetch_add(ms1_start.elapsed().as_micros() as usize, Ordering::Relaxed);
            }
            MSLevel::MS2 => {
                let ms2_start = Instant::now();
                let qs = &frame.quadrupole_settings;
                ms2_pairs.reserve(qs.isolation_mz.len());
                
                for win in 0..qs.isolation_mz.len() {
                    if win >= qs.isolation_width.len() { break; }
                    
                    // Time window calculation
                    let window_start = Instant::now();
                    let prec_mz = qs.isolation_mz[win] as f32;
                    let width = qs.isolation_width[win] as f32;
                    let low = prec_mz - width * 0.5;
                    let high = prec_mz + width * 0.5;
                    let key = (quantize(low), quantize(high));
                    MS2_WINDOW_TIME.fetch_add(window_start.elapsed().as_nanos() as usize, Ordering::Relaxed);
                    
                    let mut td = TimsTOFData::new();
                    for (p_idx, (&tof, &intensity)) in frame.tof_indices.iter().zip(frame.intensities.iter()).enumerate() {
                        let scan = find_scan_for_index(p_idx, &frame.scan_offsets);
                        if scan < qs.scan_starts[win] || scan > qs.scan_ends[win] { continue; }
                        
                        let mz = mz_cv.convert(tof as f64) as f32;
                        let im = im_cv.convert(scan as f64) as f32;
                        
                        td.rt_values_min.push(rt_min);
                        td.mobility_values.push(im);
                        td.mz_values.push(mz);
                        td.intensity_values.push(intensity);
                        td.frame_indices.push(frame.index as u32);
                        td.scan_indices.push(scan as u32);
                    }
                    ms2_pairs.push((key, td));
                }
                MS2_PROC_TIME.fetch_add(ms2_start.elapsed().as_micros() as usize, Ordering::Relaxed);
            }
            _ => {}
        }
        FrameSplit { ms1, ms2: ms2_pairs }
    }).collect();
    stats.frame_processing_ms = frame_proc_start.elapsed().as_secs_f32() * 1000.0;
    println!("  ✓ Frame processing: {:.2} ms", stats.frame_processing_ms);
    
    // Collect timing stats from atomic variables
    stats.frame_read_ms = FRAME_READ_TIME.load(Ordering::Relaxed) as f32 / 1000.0;
    stats.tof_conversion_ms = TOF_CONV_TIME.load(Ordering::Relaxed) as f32 / 1_000_000.0;
    stats.scan_lookup_ms = SCAN_LOOKUP_TIME.load(Ordering::Relaxed) as f32 / 1_000_000.0;
    stats.im_conversion_ms = IM_CONV_TIME.load(Ordering::Relaxed) as f32 / 1_000_000.0;
    stats.ms1_processing_ms = MS1_PROC_TIME.load(Ordering::Relaxed) as f32 / 1000.0;
    stats.ms2_processing_ms = MS2_PROC_TIME.load(Ordering::Relaxed) as f32 / 1000.0;
    stats.ms2_window_calc_ms = MS2_WINDOW_TIME.load(Ordering::Relaxed) as f32 / 1_000_000.0;
    
    // Count frames by type
    for split in &splits {
        if !split.ms1.mz_values.is_empty() {
            stats.ms1_frames += 1;
            stats.ms1_peaks += split.ms1.mz_values.len();
        }
        if !split.ms2.is_empty() {
            stats.ms2_frames += 1;
            for (_, td) in &split.ms2 {
                stats.ms2_peaks += td.mz_values.len();
            }
        }
    }
    
    // Step 4: Merge MS1 data
    let ms1_merge_start = Instant::now();
    println!("  Merging MS1 data...");
    let ms1_size_estimate: usize = splits.par_iter().map(|s| s.ms1.mz_values.len()).sum();
    let mut global_ms1 = TimsTOFData::with_capacity(ms1_size_estimate);
    
    for split in &splits {
        global_ms1.rt_values_min.extend(&split.ms1.rt_values_min);
        global_ms1.mobility_values.extend(&split.ms1.mobility_values);
        global_ms1.mz_values.extend(&split.ms1.mz_values);
        global_ms1.intensity_values.extend(&split.ms1.intensity_values);
        global_ms1.frame_indices.extend(&split.ms1.frame_indices);
        global_ms1.scan_indices.extend(&split.ms1.scan_indices);
    }
    stats.ms1_merge_ms = ms1_merge_start.elapsed().as_secs_f32() * 1000.0;
    println!("  ✓ MS1 merge: {:.2} ms ({} peaks)", stats.ms1_merge_ms, global_ms1.mz_values.len());
    
    // Step 5: Merge MS2 data
    let ms2_merge_start = Instant::now();
    println!("  Merging MS2 data...");
    let mut ms2_hash: HashMap<(u32,u32), TimsTOFData> = HashMap::new();
    
    for mut split in splits {
        for (key, mut td) in split.ms2 {
            ms2_hash.entry(key).or_insert_with(TimsTOFData::new).merge_from(&mut td);
        }
    }
    stats.ms2_merge_ms = ms2_merge_start.elapsed().as_secs_f32() * 1000.0;
    println!("  ✓ MS2 merge: {:.2} ms ({} windows)", stats.ms2_merge_ms, ms2_hash.len());
    
    // Step 6: Convert MS2 hash to vector
    let ms2_convert_start = Instant::now();
    let mut ms2_vec = Vec::with_capacity(ms2_hash.len());
    for ((q_low, q_high), td) in ms2_hash {
        let low = q_low as f32 / 10_000.0;
        let high = q_high as f32 / 10_000.0;
        ms2_vec.push(((low, high), td));
    }
    stats.ms2_convert_ms = ms2_convert_start.elapsed().as_secs_f32() * 1000.0;
    println!("  ✓ MS2 convert: {:.2} ms", stats.ms2_convert_ms);
    
    // Calculate memory usage
    let ms1_memory = global_ms1.estimated_memory_bytes();
    let ms2_memory: usize = ms2_vec.iter().map(|(_, td)| td.estimated_memory_bytes()).sum();
    stats.estimated_memory_mb = (ms1_memory + ms2_memory) as f32 / 1024.0 / 1024.0;
    
    println!("  MS1 data points: {}", global_ms1.mz_values.len());
    println!("  MS2 windows: {}", ms2_vec.len());
    
    Ok(TimsTOFRawData {
        ms1_data: global_ms1,
        ms2_windows: ms2_vec,
    })
}

// ============================================================================
// Main Function
// ============================================================================

fn main() -> Result<(), Box<dyn Error>> {
    // Hard-coded path to TimsTOF data - CHANGE THIS
    let data_path = "/Users/augustsirius/Desktop/DIA_peak_group_extraction/输入数据文件/raw_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d";  
    
    let d_path = Path::new(data_path);
    if !d_path.exists() {
        return Err(format!("Data folder {:?} not found", d_path).into());
    }
    
    println!("\n========== TimsTOF Raw Data Reader with Profiling ==========");
    println!("Data folder: {}", data_path);
    println!("Starting detailed performance analysis...\n");
    
    let mut stats = ProfilingStats::default();
    let total_start = Instant::now();
    
    // Read raw data with profiling
    let raw_data = read_timstof_data_profiled(d_path, &mut stats)?;
    
    stats.total_ms = total_start.elapsed().as_secs_f32() * 1000.0;
    
    // Print comprehensive report
    stats.print_report();
    
    // Additional bottleneck analysis
    println!("\n🔍 BOTTLENECK ANALYSIS:");
    let mut timings = vec![
        ("Metadata Read", stats.metadata_read_ms),
        ("Frame Reader Init", stats.frame_reader_init_ms),
        ("Frame Processing", stats.frame_processing_ms),
        ("MS1 Merge", stats.ms1_merge_ms),
        ("MS2 Merge", stats.ms2_merge_ms),
    ];
    timings.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    
    println!("Top 3 bottlenecks:");
    for (i, (name, time)) in timings.iter().take(3).enumerate() {
        println!("  {}. {} - {:.2} ms ({:.1}%)", 
            i + 1, name, time, time / stats.total_ms * 100.0);
    }
    
    // Conversion efficiency analysis
    if stats.ms1_peaks > 0 {
        let conversion_overhead = (stats.tof_conversion_ms + stats.im_conversion_ms) / stats.ms1_peaks as f32 * 1_000_000.0;
        println!("\n⚡ Conversion Efficiency:");
        println!("  • Per-peak conversion overhead: {:.3} ns", conversion_overhead);
        println!("  • Scan lookup average: {:.3} ns/peak", 
            stats.scan_lookup_ms / stats.ms1_peaks as f32 * 1_000_000.0);
    }
    
    Ok(())
}