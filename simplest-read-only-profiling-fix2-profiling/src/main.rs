use std::{error::Error, path::Path, time::{Instant, Duration}};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use rayon::prelude::*;
use timsrust::{converters::ConvertableDomain, readers::{FrameReader, MetadataReader}, MSLevel};

// ============================================================================
// Global Profiling Counters
// ============================================================================

// Frame reading statistics
static FRAME_READ_COUNT: AtomicUsize = AtomicUsize::new(0);
static FRAME_READ_TIME_US: AtomicU64 = AtomicU64::new(0);
static FRAME_READ_WAIT_TIME_US: AtomicU64 = AtomicU64::new(0);

// Processing statistics
static MS1_FRAMES_PROCESSED: AtomicUsize = AtomicUsize::new(0);
static MS2_FRAMES_PROCESSED: AtomicUsize = AtomicUsize::new(0);
static MS1_PEAKS_PROCESSED: AtomicUsize = AtomicUsize::new(0);
static MS2_PEAKS_PROCESSED: AtomicUsize = AtomicUsize::new(0);

// Memory allocation tracking
static SCAN_LOOKUP_ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static SCAN_LOOKUP_BYTES: AtomicUsize = AtomicUsize::new(0);
static TIMSTOF_DATA_ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

// MS2 merge detailed tracking
static MS2_HASHMAP_INSERTIONS: AtomicUsize = AtomicUsize::new(0);
static MS2_HASHMAP_COLLISIONS: AtomicUsize = AtomicUsize::new(0);
static MS2_VECTOR_REALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

// Thread-local timing collection
thread_local! {
    static THREAD_STATS: std::cell::RefCell<ThreadLocalStats> = std::cell::RefCell::new(ThreadLocalStats::new());
}

#[derive(Clone, Debug)]
struct ThreadLocalStats {
    thread_id: usize,
    frames_processed: usize,
    total_frame_time_us: u64,
    total_scan_lookup_time_us: u64,
    total_conversion_time_us: u64,
    peak_count: usize,
}

impl ThreadLocalStats {
    fn new() -> Self {
        Self {
            thread_id: 0,
            frames_processed: 0,
            total_frame_time_us: 0,
            total_scan_lookup_time_us: 0,
            total_conversion_time_us: 0,
            peak_count: 0,
        }
    }
}

// Global collection of thread stats
static THREAD_STATS_COLLECTOR: Mutex<Vec<ThreadLocalStats>> = Mutex::new(Vec::new());

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
        TIMSTOF_DATA_ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
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
        TIMSTOF_DATA_ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        Self {
            rt_values_min: Vec::with_capacity(capacity),
            mobility_values: Vec::with_capacity(capacity),
            mz_values: Vec::with_capacity(capacity),
            intensity_values: Vec::with_capacity(capacity),
            frame_indices: Vec::with_capacity(capacity),
            scan_indices: Vec::with_capacity(capacity),
        }
    }
    
    fn len(&self) -> usize {
        self.mz_values.len()
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
        // Track potential reallocations
        let old_cap = self.mz_values.capacity();
        
        self.rt_values_min.append(&mut other.rt_values_min);
        self.mobility_values.append(&mut other.mobility_values);
        self.mz_values.append(&mut other.mz_values);
        self.intensity_values.append(&mut other.intensity_values);
        self.frame_indices.append(&mut other.frame_indices);
        self.scan_indices.append(&mut other.scan_indices);
        
        if self.mz_values.capacity() > old_cap {
            MS2_VECTOR_REALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
    }
}

// ============================================================================
// Helper Functions with Profiling
// ============================================================================

#[inline]
fn quantize(x: f32) -> u32 {
    (x * 10_000.0).round() as u32
}

#[inline]
fn build_scan_lookup(scan_offsets: &[usize]) -> Vec<u32> {
    if scan_offsets.is_empty() {
        return Vec::new();
    }
    
    let max_index = scan_offsets.last().copied().unwrap_or(0);
    let lookup_size = max_index * std::mem::size_of::<u32>();
    
    SCAN_LOOKUP_ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
    SCAN_LOOKUP_BYTES.fetch_add(lookup_size, Ordering::Relaxed);
    
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

// ============================================================================
// Core Data Reading Function with Deep Profiling
// ============================================================================

fn read_timstof_data(d_folder: &Path) -> Result<TimsTOFRawData, Box<dyn Error>> {
    let start_time = Instant::now();
    println!("Reading TimsTOF data from: {:?}", d_folder);
    
    // Reset all counters
    FRAME_READ_COUNT.store(0, Ordering::Relaxed);
    FRAME_READ_TIME_US.store(0, Ordering::Relaxed);
    FRAME_READ_WAIT_TIME_US.store(0, Ordering::Relaxed);
    MS1_FRAMES_PROCESSED.store(0, Ordering::Relaxed);
    MS2_FRAMES_PROCESSED.store(0, Ordering::Relaxed);
    MS1_PEAKS_PROCESSED.store(0, Ordering::Relaxed);
    MS2_PEAKS_PROCESSED.store(0, Ordering::Relaxed);
    SCAN_LOOKUP_ALLOCATIONS.store(0, Ordering::Relaxed);
    SCAN_LOOKUP_BYTES.store(0, Ordering::Relaxed);
    TIMSTOF_DATA_ALLOCATIONS.store(0, Ordering::Relaxed);
    MS2_HASHMAP_INSERTIONS.store(0, Ordering::Relaxed);
    MS2_HASHMAP_COLLISIONS.store(0, Ordering::Relaxed);
    MS2_VECTOR_REALLOCATIONS.store(0, Ordering::Relaxed);
    
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
    
    // Step 3: Process frames in parallel with detailed profiling
    let frame_proc_start = Instant::now();
    println!("  Processing {} frames in parallel...", n_frames);
    
    let splits: Vec<FrameSplit> = (0..n_frames).into_par_iter().map(|idx| {
        let thread_start = Instant::now();
        
        // Measure frame read time
        let frame_wait_start = Instant::now();
        let frame_read_start = Instant::now();
        let frame = frames.get(idx).expect("frame read");
        let frame_read_duration = frame_read_start.elapsed();
        
        FRAME_READ_COUNT.fetch_add(1, Ordering::Relaxed);
        FRAME_READ_TIME_US.fetch_add(frame_read_duration.as_micros() as u64, Ordering::Relaxed);
        
        let rt_min = frame.rt_in_seconds as f32 / 60.0;
        let mut ms1 = TimsTOFData::new();
        let mut ms2_pairs: Vec<((u32,u32), TimsTOFData)> = Vec::new();
        
        let mut local_peaks = 0;
        
        match frame.ms_level {
            MSLevel::MS1 => {
                MS1_FRAMES_PROCESSED.fetch_add(1, Ordering::Relaxed);
                let n_peaks = frame.tof_indices.len();
                ms1 = TimsTOFData::with_capacity(n_peaks);
                
                // Measure scan lookup build time
                let scan_lookup_start = Instant::now();
                let scan_lookup = build_scan_lookup(&frame.scan_offsets);
                let scan_lookup_duration = scan_lookup_start.elapsed();
                
                // Measure conversion time
                let conversion_start = Instant::now();
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
                    local_peaks += 1;
                }
                let conversion_duration = conversion_start.elapsed();
                
                MS1_PEAKS_PROCESSED.fetch_add(n_peaks, Ordering::Relaxed);
                
                // Update thread-local stats
                THREAD_STATS.with(|stats| {
                    let mut s = stats.borrow_mut();
                    s.frames_processed += 1;
                    s.total_scan_lookup_time_us += scan_lookup_duration.as_micros() as u64;
                    s.total_conversion_time_us += conversion_duration.as_micros() as u64;
                    s.peak_count += local_peaks;
                });
            }
            MSLevel::MS2 => {
                MS2_FRAMES_PROCESSED.fetch_add(1, Ordering::Relaxed);
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
                    
                    let estimated_peaks = frame.tof_indices.len() / qs.isolation_mz.len();
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
                        local_peaks += 1;
                    }
                    
                    if !td.mz_values.is_empty() {
                        MS2_PEAKS_PROCESSED.fetch_add(td.len(), Ordering::Relaxed);
                        ms2_pairs.push((key, td));
                    }
                }
                
                THREAD_STATS.with(|stats| {
                    let mut s = stats.borrow_mut();
                    s.frames_processed += 1;
                    s.peak_count += local_peaks;
                });
            }
            _ => {}
        }
        
        let thread_duration = thread_start.elapsed();
        THREAD_STATS.with(|stats| {
            let mut s = stats.borrow_mut();
            s.total_frame_time_us += thread_duration.as_micros() as u64;
        });
        
        FrameSplit { ms1, ms2: ms2_pairs }
    }).collect();
    
    println!("  ✓ Frame processing: {:.2} ms", frame_proc_start.elapsed().as_secs_f32() * 1000.0);
    
    // Collect thread-local statistics
    THREAD_STATS.with(|stats| {
        let s = stats.borrow().clone();
        if s.frames_processed > 0 {
            THREAD_STATS_COLLECTOR.lock().unwrap().push(s);
        }
    });
    
    // Step 4: Merge MS1 data
    let ms1_merge_start = Instant::now();
    println!("  Merging MS1 data...");
    
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
    
    // Step 5: Merge MS2 data - WITH DETAILED PROFILING
    let ms2_merge_start = Instant::now();
    println!("  Merging MS2 data with detailed profiling...");
    
    // Method 1: Direct HashMap merge (original)
    let method1_start = Instant::now();
    let mut ms2_hash: HashMap<(u32,u32), TimsTOFData> = HashMap::with_capacity(64);
    
    for mut split in splits {
        for (key, mut td) in split.ms2 {
            MS2_HASHMAP_INSERTIONS.fetch_add(1, Ordering::Relaxed);
            
            let entry = ms2_hash.entry(key).or_insert_with(|| {
                TimsTOFData::with_capacity(50000) // Pre-allocate for typical window size
            });
            
            // Check if this is a collision (key already existed)
            if entry.len() > 0 {
                MS2_HASHMAP_COLLISIONS.fetch_add(1, Ordering::Relaxed);
            }
            
            entry.merge_from(&mut td);
        }
    }
    let method1_duration = method1_start.elapsed();
    
    println!("  ✓ MS2 merge: {:.2} ms ({} windows)", 
             ms2_merge_start.elapsed().as_secs_f32() * 1000.0, 
             ms2_hash.len());
    println!("    Method duration: {:.2} ms", method1_duration.as_secs_f32() * 1000.0);
    
    // Step 6: Convert to final format
    let ms2_convert_start = Instant::now();
    let mut ms2_vec = Vec::with_capacity(ms2_hash.len());
    
    for ((q_low, q_high), td) in ms2_hash {
        let low = q_low as f32 / 10_000.0;
        let high = q_high as f32 / 10_000.0;
        ms2_vec.push(((low, high), td));
    }
    
    ms2_vec.sort_by(|a, b| a.0.0.partial_cmp(&b.0.0).unwrap());
    
    println!("  ✓ MS2 convert: {:.2} ms", ms2_convert_start.elapsed().as_secs_f32() * 1000.0);
    
    // Print detailed profiling report
    print_profiling_report();
    
    Ok(TimsTOFRawData {
        ms1_data: global_ms1,
        ms2_windows: ms2_vec,
    })
}

fn print_profiling_report() {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║              DEEP PROFILING REPORT                            ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    
    // Frame reading statistics
    let frame_count = FRAME_READ_COUNT.load(Ordering::Relaxed);
    let total_read_time_us = FRAME_READ_TIME_US.load(Ordering::Relaxed);
    let avg_read_time_us = if frame_count > 0 { total_read_time_us / frame_count as u64 } else { 0 };
    
    println!("║ FRAME READING:                                                ║");
    println!("║   Total frames read: {}                                    ║", frame_count);
    println!("║   Total read time: {:.2} ms                              ║", total_read_time_us as f32 / 1000.0);
    println!("║   Average per frame: {} µs                               ║", avg_read_time_us);
    
    // Processing statistics
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║ PROCESSING STATISTICS:                                        ║");
    println!("║   MS1 frames: {}                                          ║", MS1_FRAMES_PROCESSED.load(Ordering::Relaxed));
    println!("║   MS2 frames: {}                                         ║", MS2_FRAMES_PROCESSED.load(Ordering::Relaxed));
    println!("║   MS1 peaks: {}                                      ║", MS1_PEAKS_PROCESSED.load(Ordering::Relaxed));
    println!("║   MS2 peaks: {}                                     ║", MS2_PEAKS_PROCESSED.load(Ordering::Relaxed));
    
    // Memory allocation statistics
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║ MEMORY ALLOCATIONS:                                           ║");
    let scan_lookups = SCAN_LOOKUP_ALLOCATIONS.load(Ordering::Relaxed);
    let scan_bytes = SCAN_LOOKUP_BYTES.load(Ordering::Relaxed);
    println!("║   Scan lookup tables: {}                                 ║", scan_lookups);
    println!("║   Scan lookup memory: {:.2} MB                           ║", scan_bytes as f32 / 1024.0 / 1024.0);
    println!("║   TimsTOFData objects: {}                                ║", TIMSTOF_DATA_ALLOCATIONS.load(Ordering::Relaxed));
    
    // MS2 merge statistics
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║ MS2 MERGE DETAILS:                                            ║");
    println!("║   HashMap insertions: {}                                ║", MS2_HASHMAP_INSERTIONS.load(Ordering::Relaxed));
    println!("║   HashMap collisions: {}                                ║", MS2_HASHMAP_COLLISIONS.load(Ordering::Relaxed));
    println!("║   Vector reallocations: {}                                ║", MS2_VECTOR_REALLOCATIONS.load(Ordering::Relaxed));
    
    // Thread statistics
    let thread_stats = THREAD_STATS_COLLECTOR.lock().unwrap();
    if !thread_stats.is_empty() {
        println!("╠══════════════════════════════════════════════════════════════╣");
        println!("║ THREAD EFFICIENCY:                                            ║");
        println!("║   Active threads: {}                                      ║", thread_stats.len());
        
        let total_frame_time: u64 = thread_stats.iter().map(|s| s.total_frame_time_us).sum();
        let total_frames: usize = thread_stats.iter().map(|s| s.frames_processed).sum();
        let avg_time_per_thread = total_frame_time / thread_stats.len() as u64;
        
        println!("║   Avg time per thread: {:.2} ms                          ║", avg_time_per_thread as f32 / 1000.0);
        println!("║   Load balance (frames/thread):                               ║");
        
        // Show distribution of work
        let min_frames = thread_stats.iter().map(|s| s.frames_processed).min().unwrap_or(0);
        let max_frames = thread_stats.iter().map(|s| s.frames_processed).max().unwrap_or(0);
        let avg_frames = total_frames / thread_stats.len();
        
        println!("║     Min: {} | Avg: {} | Max: {}                    ║", min_frames, avg_frames, max_frames);
        
        if max_frames > min_frames * 2 {
            println!("║   ⚠️  WARNING: Significant load imbalance detected!           ║");
        }
    }
    
    println!("╚══════════════════════════════════════════════════════════════╝");
}

// ============================================================================
// Main Function
// ============================================================================

fn main() -> Result<(), Box<dyn Error>> {
    // Test with different thread counts
    let thread_counts = vec![60];
    let data_path = "/wangshuaiyao/dia-bert-timstof/test_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d";
    // let data_path = "/Users/augustsirius/Desktop/DIA_peak_group_extraction/输入数据文件/raw_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d";
    
    let d_path = Path::new(data_path);
    
    println!("\n========== SCALING ANALYSIS WITH DEEP PROFILING ==========");
    println!("Testing thread counts: {:?}", thread_counts);
    println!("Data: {}", data_path);
    
    let mut results = Vec::new();
    
    for num_threads in thread_counts {
        println!("\n╔══════════════════════════════════════════════════════════════╗");
        println!("║ Testing with {} threads", num_threads);
        println!("╚══════════════════════════════════════════════════════════════╝");
        
        // Configure thread pool
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()
            .unwrap();
        
        println!("Thread pool size: {}", rayon::current_num_threads());
        
        let start_time = Instant::now();
        let raw_data = read_timstof_data(d_path)?;
        let elapsed = start_time.elapsed();
        
        results.push((num_threads, elapsed.as_secs_f32()));
        
        println!("\nTotal time with {} threads: {:.2} seconds", num_threads, elapsed.as_secs_f32());
        
        // Calculate throughput
        let total_peaks = raw_data.ms1_data.mz_values.len() + 
            raw_data.ms2_windows.iter().map(|(_, td)| td.mz_values.len()).sum::<usize>();
        println!("Throughput: {:.0} peaks/second", total_peaks as f32 / elapsed.as_secs_f32());
    }
    
    // Print scaling summary
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║                    SCALING SUMMARY                            ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    for (threads, time) in &results {
        let speedup = results[0].1 / time;
        let efficiency = speedup / (*threads as f32 / results[0].0 as f32) * 100.0;
        println!("║ {} threads: {:.2}s | Speedup: {:.2}x | Efficiency: {:.1}% ║", 
                 threads, time, speedup, efficiency);
    }
    println!("╚══════════════════════════════════════════════════════════════╝");
    
    Ok(())
}