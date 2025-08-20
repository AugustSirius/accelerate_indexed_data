use std::{error::Error, path::Path, time::Instant};
use std::collections::HashMap;
use rayon::prelude::*;
use timsrust::{converters::ConvertableDomain, readers::{FrameReader, MetadataReader}, MSLevel, Frame};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};

// ============================================================================
// CPU Usage Monitoring
// ============================================================================

static TOTAL_CPU_TIME_NS: AtomicU64 = AtomicU64::new(0);
static ACTIVE_THREADS: AtomicUsize = AtomicUsize::new(0);
static PEAK_THREADS: AtomicUsize = AtomicUsize::new(0);

fn update_peak_threads() {
    let current = ACTIVE_THREADS.fetch_add(1, Ordering::Relaxed) + 1;
    let mut peak = PEAK_THREADS.load(Ordering::Relaxed);
    while current > peak {
        match PEAK_THREADS.compare_exchange_weak(peak, current, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(x) => peak = x,
        }
    }
}

fn track_cpu_time<F, R>(f: F) -> R 
where 
    F: FnOnce() -> R 
{
    update_peak_threads();
    let start = Instant::now();
    let result = f();
    let elapsed = start.elapsed().as_nanos() as u64;
    TOTAL_CPU_TIME_NS.fetch_add(elapsed, Ordering::Relaxed);
    ACTIVE_THREADS.fetch_sub(1, Ordering::Relaxed);
    result
}

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

// ============================================================================
// Helper Functions
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

// Process a single frame (pure computation, no I/O)
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
// Core Data Reading Function - FINAL OPTIMIZED VERSION
// ============================================================================

fn read_timstof_data(d_folder: &Path) -> Result<TimsTOFRawData, Box<dyn Error>> {
    let total_start = Instant::now();
    
    // Reset CPU monitoring
    TOTAL_CPU_TIME_NS.store(0, Ordering::Relaxed);
    ACTIVE_THREADS.store(0, Ordering::Relaxed);
    PEAK_THREADS.store(0, Ordering::Relaxed);
    
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║         TIMSTOF DATA READER - PRODUCTION VERSION              ║");
    println!("║         Thread Pool: 64 threads                               ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!("\nData folder: {:?}", d_folder);
    
    // Step 1: Read metadata
    println!("\n[Phase 1: Metadata]");
    let metadata_start = Instant::now();
    let tdf_path = d_folder.join("analysis.tdf");
    let meta = MetadataReader::new(&tdf_path)?;
    let mz_cv = Arc::new(meta.mz_converter);
    let im_cv = Arc::new(meta.im_converter);
    println!("  ✓ Metadata read: {:.2} ms", metadata_start.elapsed().as_secs_f32() * 1000.0);
    
    // Step 2: Initialize frame reader
    println!("\n[Phase 2: Frame Reader Initialization]");
    let frame_init_start = Instant::now();
    let frames = FrameReader::new(d_folder)?;
    let n_frames = frames.len();
    println!("  ✓ Frame reader initialized: {:.2} ms", 
             frame_init_start.elapsed().as_secs_f32() * 1000.0);
    println!("  ✓ Total frames: {}", n_frames);
    
    // ============================================================================
    // CRITICAL: Sequential frame reading to eliminate contention
    // ============================================================================
    println!("\n[Phase 3: Sequential Frame Reading]");
    println!("  Reading {} frames sequentially (no contention)...", n_frames);
    let frame_read_start = Instant::now();
    
    let mut all_frames = Vec::with_capacity(n_frames);
    let progress_interval = 5000;
    
    for idx in 0..n_frames {
        all_frames.push(frames.get(idx).expect("frame read"));
        
        if (idx + 1) % progress_interval == 0 || idx == n_frames - 1 {
            let progress = (idx + 1) as f32 / n_frames as f32 * 100.0;
            print!("\r  Progress: {}/{} frames ({:.1}%)", idx + 1, n_frames, progress);
            use std::io::{self, Write};
            io::stdout().flush().unwrap();
        }
    }
    
    let frame_read_time = frame_read_start.elapsed().as_secs_f32();
    println!("\n  ✓ Frame reading complete: {:.2} seconds", frame_read_time);
    println!("  ✓ Average: {:.2} ms/frame", frame_read_time * 1000.0 / n_frames as f32);
    
    // Estimate memory usage of frames
    let frame_memory_mb = all_frames.len() * std::mem::size_of::<Frame>() / 1024 / 1024;
    println!("  ✓ Frame memory usage: ~{} MB", frame_memory_mb);
    
    // ============================================================================
    // Parallel processing (pure computation, no I/O)
    // ============================================================================
    println!("\n[Phase 4: Parallel Frame Processing]");
    println!("  Processing {} frames with 64 threads...", n_frames);
    let frame_proc_start = Instant::now();
    
    let splits: Vec<FrameSplit> = all_frames
        .into_par_iter()
        .map(|frame| {
            track_cpu_time(|| process_frame(frame, &mz_cv, &im_cv))
        })
        .collect();
    
    let frame_proc_time = frame_proc_start.elapsed().as_secs_f32();
    let total_cpu_time_sec = TOTAL_CPU_TIME_NS.load(Ordering::Relaxed) as f64 / 1_000_000_000.0;
    let cpu_efficiency = (total_cpu_time_sec / 64.0) / frame_proc_time as f64 * 100.0;
    let peak_threads_used = PEAK_THREADS.load(Ordering::Relaxed);
    
    println!("  ✓ Frame processing complete: {:.2} seconds", frame_proc_time);
    println!("  ✓ Total CPU time: {:.2} seconds", total_cpu_time_sec);
    println!("  ✓ CPU efficiency: {:.1}%", cpu_efficiency);
    println!("  ✓ Peak threads active: {}/64", peak_threads_used);
    println!("  ✓ Speedup: {:.1}x", total_cpu_time_sec / frame_proc_time as f64);
    
    // Count MS1 and MS2 frames
    let ms1_frame_count = splits.iter().filter(|s| !s.ms1.mz_values.is_empty()).count();
    let ms2_frame_count = splits.iter().filter(|s| !s.ms2.is_empty()).count();
    println!("  ✓ MS1 frames: {} | MS2 frames: {}", ms1_frame_count, ms2_frame_count);
    
    // ============================================================================
    // MS1 Merge
    // ============================================================================
    println!("\n[Phase 5: MS1 Data Merge]");
    let ms1_merge_start = Instant::now();
    
    // Calculate exact size for single allocation
    let ms1_total_size: usize = splits.par_iter()
        .map(|s| s.ms1.mz_values.len())
        .sum();
    
    println!("  Pre-allocating for {} MS1 peaks...", ms1_total_size);
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
    
    let ms1_merge_time = ms1_merge_start.elapsed().as_secs_f32();
    println!("  ✓ MS1 merge complete: {:.2} seconds", ms1_merge_time);
    println!("  ✓ MS1 peaks: {}", global_ms1.mz_values.len());
    
    // ============================================================================
    // MS2 Merge - Optimized without HashMap contention
    // ============================================================================
    println!("\n[Phase 6: MS2 Data Merge]");
    let ms2_merge_start = Instant::now();
    
    // Count total MS2 fragments
    let total_ms2_fragments: usize = splits.iter()
        .map(|s| s.ms2.len())
        .sum();
    println!("  Processing {} MS2 fragments...", total_ms2_fragments);
    
    // Collect all MS2 data
    let mut window_map: HashMap<(u32, u32), Vec<TimsTOFData>> = HashMap::with_capacity(64);
    
    for split in splits {
        for (key, data) in split.ms2 {
            window_map.entry(key)
                .or_insert_with(Vec::new)
                .push(data);
        }
    }
    
    println!("  Merging into {} unique windows...", window_map.len());
    
    // Pre-calculate total sizes per window for perfect allocation
    let mut window_sizes: HashMap<(u32, u32), usize> = HashMap::with_capacity(window_map.len());
    for (key, data_vec) in &window_map {
        let total: usize = data_vec.iter().map(|d| d.mz_values.len()).sum();
        window_sizes.insert(*key, total);
    }
    
    // Merge with exact pre-allocation
    let mut ms2_final: Vec<((f32, f32), TimsTOFData)> = Vec::with_capacity(window_map.len());
    
    for ((q_low, q_high), mut data_vec) in window_map {
        let total_size = window_sizes[&(q_low, q_high)];
        
        if data_vec.len() == 1 {
            // Optimization: single fragment, just move it
            let low = q_low as f32 / 10_000.0;
            let high = q_high as f32 / 10_000.0;
            ms2_final.push(((low, high), data_vec.into_iter().next().unwrap()));
        } else {
            // Multiple fragments: merge with exact allocation
            let mut merged = TimsTOFData::with_capacity(total_size);
            
            // Use extend for better cache performance
            for data in &mut data_vec {
                merged.rt_values_min.extend(&data.rt_values_min);
                merged.mobility_values.extend(&data.mobility_values);
                merged.mz_values.extend(&data.mz_values);
                merged.intensity_values.extend(&data.intensity_values);
                merged.frame_indices.extend(&data.frame_indices);
                merged.scan_indices.extend(&data.scan_indices);
            }
            
            let low = q_low as f32 / 10_000.0;
            let high = q_high as f32 / 10_000.0;
            ms2_final.push(((low, high), merged));
        }
    }
    
    // Sort by window for consistent output
    ms2_final.sort_by(|a, b| a.0.0.partial_cmp(&b.0.0).unwrap());
    
    let ms2_merge_time = ms2_merge_start.elapsed().as_secs_f32();
    let total_ms2_peaks: usize = ms2_final.iter().map(|(_, td)| td.mz_values.len()).sum();
    
    println!("  ✓ MS2 merge complete: {:.2} seconds", ms2_merge_time);
    println!("  ✓ MS2 windows: {}", ms2_final.len());
    println!("  ✓ MS2 peaks: {}", total_ms2_peaks);
    
    // ============================================================================
    // Final Statistics
    // ============================================================================
    let total_time = total_start.elapsed().as_secs_f32();
    let total_peaks = global_ms1.mz_values.len() + total_ms2_peaks;
    
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║                     FINAL STATISTICS                          ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║ Phase                   │ Time (s)  │ % of Total              ║");
    println!("╟─────────────────────────┼───────────┼─────────────────────────╢");
    println!("║ Frame Reading (I/O)     │ {:>9.2} │ {:>5.1}%                  ║", 
             frame_read_time, frame_read_time / total_time * 100.0);
    println!("║ Frame Processing (CPU)  │ {:>9.2} │ {:>5.1}%                  ║", 
             frame_proc_time, frame_proc_time / total_time * 100.0);
    println!("║ MS1 Merge               │ {:>9.2} │ {:>5.1}%                  ║", 
             ms1_merge_time, ms1_merge_time / total_time * 100.0);
    println!("║ MS2 Merge               │ {:>9.2} │ {:>5.1}%                  ║", 
             ms2_merge_time, ms2_merge_time / total_time * 100.0);
    println!("╟─────────────────────────┼───────────┼─────────────────────────╢");
    println!("║ TOTAL                   │ {:>9.2} │ 100.0%                  ║", total_time);
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║ Total Peaks Processed   │ {:>27}           ║", total_peaks);
    println!("║ Throughput              │ {:>20.0} peaks/s     ║", total_peaks as f32 / total_time);
    println!("║ Data Rate               │ {:>20.2} GB/s        ║", 
             (total_peaks * 24) as f32 / 1024.0 / 1024.0 / 1024.0 / total_time);
    println!("╚══════════════════════════════════════════════════════════════╝");
    
    println!("\n✨ Processing complete!");
    
    Ok(TimsTOFRawData {
        ms1_data: global_ms1,
        ms2_windows: ms2_final,
    })
}

// ============================================================================
// Main Function
// ============================================================================

fn main() -> Result<(), Box<dyn Error>> {
    // Configure thread pool for 64 threads
    const NUM_THREADS: usize = 64;
    
    rayon::ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .thread_name(|i| format!("worker-{:02}", i))
        .build_global()
        .unwrap();
    
    // Data path - CHANGE THIS
    let data_path = "/wangshuaiyao/dia-bert-timstof/test_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d";
    // let data_path = "/Users/augustsirius/Desktop/DIA_peak_group_extraction/输入数据文件/raw_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d";
    
    let d_path = Path::new(data_path);
    
    // Run the optimized reader
    let start = Instant::now();
    let raw_data = read_timstof_data(d_path)?;
    let elapsed = start.elapsed();
    
    // Verify output
    println!("\n[Verification]");
    println!("  MS1 peaks: {}", raw_data.ms1_data.mz_values.len());
    println!("  MS2 windows: {}", raw_data.ms2_windows.len());
    let total_ms2: usize = raw_data.ms2_windows.iter()
        .map(|(_, td)| td.mz_values.len())
        .sum();
    println!("  MS2 peaks: {}", total_ms2);
    println!("  Total execution time: {:.2} seconds", elapsed.as_secs_f32());
    
    Ok(())
}