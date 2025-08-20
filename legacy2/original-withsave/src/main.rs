#[cfg(feature = "mimalloc")]
use mimalloc::MiMalloc;

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::Instant;
use std::env;

use rayon::prelude::*;
use timsrust::{converters::ConvertableDomain, readers::{FrameReader, MetadataReader}, MSLevel};
use timsrust::converters::{Tof2MzConverter, Scan2ImConverter};
use serde::{Serialize, Deserialize};
use dashmap::DashMap;
use crossbeam_channel::bounded;
use parking_lot::Mutex;

const NUM_THREADS: usize = 64;
const CHANNEL_BUFFER_SIZE: usize = 2000;

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
    
    fn merge_from(&mut self, other: TimsTOFData) {
        self.rt_values_min.extend(other.rt_values_min);
        self.mobility_values.extend(other.mobility_values);
        self.mz_values.extend(other.mz_values);
        self.intensity_values.extend(other.intensity_values);
        self.frame_indices.extend(other.frame_indices);
        self.scan_indices.extend(other.scan_indices);
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
// Data Loading
// ============================================================================

#[derive(Clone)]
enum ProcessedFrame {
    MS1(usize, TimsTOFData),
    MS2(usize, Vec<((u32, u32), TimsTOFData)>),
}

struct FrameProcessor {
    mz_cv: Arc<Tof2MzConverter>,
    im_cv: Arc<Scan2ImConverter>,
}

impl FrameProcessor {
    #[inline(always)]
    fn process_peaks_batch(
        &self,
        tof_indices: &[u32],
        intensities: &[u32],
        scan_offsets: &[usize],
        rt_min: f32,
        frame_index: u32,
        scan_filter: Option<(usize, usize)>,
    ) -> TimsTOFData {
        let n_peaks = tof_indices.len();
        let mut data = TimsTOFData::with_capacity(n_peaks);
        
        for (p_idx, (&tof, &intensity)) in tof_indices.iter().zip(intensities.iter()).enumerate() {
            let scan = find_scan_for_index(p_idx, scan_offsets);
            
            if let Some((start, end)) = scan_filter {
                if scan < start || scan > end { continue; }
            }
            
            let mz = self.mz_cv.convert(tof as f64) as f32;
            let im = self.im_cv.convert(scan as f64) as f32;
            
            data.rt_values_min.push(rt_min);
            data.mobility_values.push(im);
            data.mz_values.push(mz);
            data.intensity_values.push(intensity);
            data.frame_indices.push(frame_index);
            data.scan_indices.push(scan as u32);
        }
        
        data
    }
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
    
    let (sender, receiver) = bounded(CHANNEL_BUFFER_SIZE);
    let processed_count = Arc::new(AtomicUsize::new(0));
    
    let ms1_accumulator = Arc::new(Mutex::new(Vec::with_capacity(n_frames)));
    let ms2_map = Arc::new(DashMap::with_capacity(100));
    
    let ms1_acc_clone = Arc::clone(&ms1_accumulator);
    let ms2_map_clone = Arc::clone(&ms2_map);
    let processed_clone = Arc::clone(&processed_count);
    
    let aggregator_handle = std::thread::spawn(move || {
        let mut frame_buffer: Vec<Option<ProcessedFrame>> = vec![None; n_frames];
        let mut next_frame = 0usize;
        
        while let Ok(frame_data) = receiver.recv() {
            match frame_data {
                ProcessedFrame::MS1(idx, data) => {
                    frame_buffer[idx] = Some(ProcessedFrame::MS1(idx, data));
                }
                ProcessedFrame::MS2(idx, pairs) => {
                    frame_buffer[idx] = Some(ProcessedFrame::MS2(idx, pairs));
                }
            }
            
            while next_frame < n_frames {
                if let Some(frame) = frame_buffer[next_frame].take() {
                    match frame {
                        ProcessedFrame::MS1(_, data) => {
                            if !data.mz_values.is_empty() {
                                ms1_acc_clone.lock().push(data);
                            }
                        }
                        ProcessedFrame::MS2(_, pairs) => {
                            for (key, data) in pairs {
                                if !data.mz_values.is_empty() {
                                    ms2_map_clone.entry(key)
                                        .or_insert_with(|| Arc::new(Mutex::new(TimsTOFData::new())))
                                        .lock()
                                        .merge_from(data);
                                }
                            }
                        }
                    }
                    next_frame += 1;
                    processed_clone.fetch_add(1, AtomicOrdering::Relaxed);
                } else {
                    break;
                }
            }
        }
        
        for frame in frame_buffer.into_iter().skip(next_frame) {
            if let Some(frame_data) = frame {
                match frame_data {
                    ProcessedFrame::MS1(_, data) => {
                        if !data.mz_values.is_empty() {
                            ms1_acc_clone.lock().push(data);
                        }
                    }
                    ProcessedFrame::MS2(_, pairs) => {
                        for (key, data) in pairs {
                            if !data.mz_values.is_empty() {
                                ms2_map_clone.entry(key)
                                    .or_insert_with(|| Arc::new(Mutex::new(TimsTOFData::new())))
                                    .lock()
                                    .merge_from(data);
                            }
                        }
                    }
                }
                processed_clone.fetch_add(1, AtomicOrdering::Relaxed);
            }
        }
    });
    
    (0..n_frames).into_par_iter().for_each(|idx| {
        let processor = FrameProcessor {
            mz_cv: Arc::clone(&mz_cv),
            im_cv: Arc::clone(&im_cv),
        };
        
        let frame = match frames.get(idx) {
            Ok(f) => f,
            Err(_) => return,
        };
        
        let rt_min = frame.rt_in_seconds as f32 / 60.0;
        
        match frame.ms_level {
            MSLevel::MS1 => {
                let ms1 = processor.process_peaks_batch(
                    &frame.tof_indices,
                    &frame.intensities,
                    &frame.scan_offsets,
                    rt_min,
                    frame.index as u32,
                    None,
                );
                
                if !ms1.mz_values.is_empty() {
                    let _ = sender.send(ProcessedFrame::MS1(idx, ms1));
                }
            }
            MSLevel::MS2 => {
                let qs = &frame.quadrupole_settings;
                let mut ms2_pairs = Vec::with_capacity(qs.isolation_mz.len());
                
                for win in 0..qs.isolation_mz.len() {
                    if win >= qs.isolation_width.len() { break; }
                    
                    let prec_mz = qs.isolation_mz[win] as f32;
                    let width = qs.isolation_width[win] as f32;
                    let low = prec_mz - width * 0.5;
                    let high = prec_mz + width * 0.5;
                    let key = (quantize(low), quantize(high));
                    
                    let td = processor.process_peaks_batch(
                        &frame.tof_indices,
                        &frame.intensities,
                        &frame.scan_offsets,
                        rt_min,
                        frame.index as u32,
                        Some((qs.scan_starts[win], qs.scan_ends[win])),
                    );
                    
                    if !td.mz_values.is_empty() {
                        ms2_pairs.push((key, td));
                    }
                }
                
                if !ms2_pairs.is_empty() {
                    let _ = sender.send(ProcessedFrame::MS2(idx, ms2_pairs));
                }
            }
            _ => {}
        }
    });
    
    drop(sender);
    aggregator_handle.join().unwrap();
    
    println!("  Frame processing: {:.3}s", process_start.elapsed().as_secs_f32());
    println!("  Frames processed: {}", processed_count.load(AtomicOrdering::Relaxed));
    
    println!("Finalizing data structures...");
    let finalize_start = Instant::now();
    
    let ms1_chunks = ms1_accumulator.lock();
    let actual_ms1_size: usize = ms1_chunks.iter().map(|c| c.mz_values.len()).sum();
    let mut global_ms1 = TimsTOFData::preallocate_exact(actual_ms1_size);
    
    for mut chunk in ms1_chunks.clone() {
        unsafe { global_ms1.append_unchecked(&mut chunk); }
    }
    
    let mut ms2_vec = Vec::with_capacity(ms2_map.len());
    ms2_map.iter().for_each(|entry| {
        let (q_low, q_high) = *entry.key();
        let low = q_low as f32 / 10_000.0;
        let high = q_high as f32 / 10_000.0;
        let data = entry.value().lock().clone();
        ms2_vec.push(((low, high), data));
    });
    
    println!("  Data finalization: {:.3}s", finalize_start.elapsed().as_secs_f32());
    println!("Total loading time: {:.3}s", total_start.elapsed().as_secs_f32());
    
    Ok(TimsTOFRawData {
        ms1_data: global_ms1,
        ms2_windows: ms2_vec,
    })
}

// ============================================================================
// Indexing
// ============================================================================

impl IndexedTimsTOFData {
    pub fn from_timstof_data(data: TimsTOFData) -> Self {
        let n_peaks = data.mz_values.len();
        
        const PARALLEL_THRESHOLD: usize = 10_000;
        
        if n_peaks < PARALLEL_THRESHOLD {
            Self::from_timstof_data_sequential(data)
        } else {
            Self::from_timstof_data_parallel(data)
        }
    }
    
    fn from_timstof_data_sequential(data: TimsTOFData) -> Self {
        let n_peaks = data.mz_values.len();
        
        let mut order: Vec<usize> = (0..n_peaks).collect();
        order.sort_by(|&a, &b| data.mz_values[a].partial_cmp(&data.mz_values[b]).unwrap());

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
    
    fn from_timstof_data_parallel(data: TimsTOFData) -> Self {
        let n_peaks = data.mz_values.len();
        
        let mut indexed_mz: Vec<(usize, f32)> = (0..n_peaks)
            .into_par_iter()
            .map(|i| (i, data.mz_values[i]))
            .collect();
        
        indexed_mz.par_sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        
        let order: Vec<usize> = indexed_mz.into_par_iter()
            .map(|(idx, _)| idx)
            .collect();
        
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
    
    fn reorder_f32_parallel(src: &[f32], order: &[usize]) -> Vec<f32> {
        order.par_iter().map(|&i| src[i]).collect()
    }
    
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

#[inline]
pub fn quantize(x: f32) -> u32 { 
    (x * 10_000.0).round() as u32 
}

pub fn find_scan_for_index(index: usize, scan_offsets: &[usize]) -> usize {
    for (scan, window) in scan_offsets.windows(2).enumerate() {
        if index >= window[0] && index < window[1] {
            return scan;
        }
    }
    scan_offsets.len() - 1
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
    
    let args: Vec<String> = env::args().collect();
    
    // Set default data folder based on OS
    let default_data_folder = if std::env::consts::OS == "macos" {
        "/Users/augustsirius/Desktop/DIA_peak_group_extraction/输入数据文件/raw_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d"
    } else {
        "/wangshuaiyao/dia-bert-timstof/test_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d"
    };
    
    let d_folder = args.get(1).cloned().unwrap_or_else(|| default_data_folder.to_string());
    let d_path = Path::new(&d_folder);
    
    if !d_path.exists() {
        return Err(format!("Folder {:?} not found", d_path).into());
    }
    
    println!("\n========== TIMSTOF DATA LOADER ==========");
    println!("Data folder: {}", d_folder);
    println!("Threads: {}", NUM_THREADS);
    
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
    
    // Final summary
    println!("\n========== COMPLETE ==========");
    println!("Total time: {:.3}s", load_time + index_time);
    println!("Data successfully loaded and indexed!");
    
    Ok(())
}