use std::collections::HashMap;
use ndarray::{Array2, Array3, s};
use std::cmp::Ordering;
use std::error::Error;
use polars::prelude::*;
use std::fs::File;
use rayon::prelude::*;
use rayon::slice::ParallelSliceMut;
use csv::ReaderBuilder;
use std::path::Path;
use std::sync::Arc;
use timsrust::{converters::ConvertableDomain, readers::{FrameReader, MetadataReader}, MSLevel};
use timsrust::converters::{Tof2MzConverter, Scan2ImConverter};
use serde::{Serialize, Deserialize};
use dashmap::DashMap;
use crossbeam_channel::bounded;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::Instant;
use mimalloc::MiMalloc;

// Global allocator
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

// Constants
const NUM_THREADS: usize = 64;  // From main.rs parallel_threads setting
const CHANNEL_BUFFER_SIZE: usize = 2000;

#[derive(Debug, Clone)]
pub struct PrecursorLibData {
    pub precursor_id: String,
    pub im: f32,
    pub rt: f32,
    pub lib_records: Vec<LibraryRecord>,
    pub ms1_data: MSDataArray,
    pub ms2_data: MSDataArray,
    pub precursor_info: Vec<f32>,
}

pub fn prepare_precursor_lib_data(
    library_records: &[LibraryRecord],
    diann_precursor_ids: &[String],
    assay_rt_dict: &HashMap<String, f32>,
    assay_im_dict: &HashMap<String, f32>,
    lib_cols: &LibCols,
    max_precursors: usize,
) -> Result<Vec<PrecursorLibData>, Box<dyn Error>> {
    // 获取前N个unique precursor IDs
    let unique_precursors: Vec<String> = diann_precursor_ids
        .iter()
        .take(max_precursors)
        .cloned()
        .collect();
    
    println!("Preparing library data for {} precursors...", unique_precursors.len());
    
    // 并行处理每个precursor
    let precursor_data_list: Vec<PrecursorLibData> = unique_precursors
        .par_iter()
        .filter_map(|precursor_id| {
            // 获取该precursor的所有library records
            let each_lib_data: Vec<LibraryRecord> = library_records
                .iter()
                .filter(|record| &record.transition_group_id == precursor_id)
                .cloned()
                .collect();
            
            if each_lib_data.is_empty() {
                return None;
            }
            
            // 获取RT和IM
            let rt = assay_rt_dict.get(precursor_id).copied().unwrap_or(0.0);
            let im = assay_im_dict.get(precursor_id).copied().unwrap_or(0.0);
            
            // 构建library matrices
            match build_lib_matrix(&each_lib_data, lib_cols, 5.0, 1801.0, 20) {
                Ok((precursors_list, ms1_data_list, ms2_data_list, precursor_info_list)) => {
                    if !precursors_list.is_empty() {
                        Some(PrecursorLibData {
                            precursor_id: precursor_id.clone(),
                            im,
                            rt,
                            lib_records: each_lib_data,
                            ms1_data: ms1_data_list[0].clone(),
                            ms2_data: ms2_data_list[0].clone(),
                            precursor_info: precursor_info_list[0].clone(),
                        })
                    } else {
                        None
                    }
                },
                Err(_) => None,
            }
        })
        .collect();
    
    Ok(precursor_data_list)
}

// ============================================================================
// TimsTOF 数据读取相关结构体和函数
// ============================================================================

// TimsTOF数据结构 - Single definition with all methods
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
    
    pub fn merge(data_list: Vec<TimsTOFData>) -> Self {
        let mut merged = TimsTOFData::new();
        
        for data in data_list {
            merged.rt_values_min.extend(data.rt_values_min);
            merged.mobility_values.extend(data.mobility_values);
            merged.mz_values.extend(data.mz_values);
            merged.intensity_values.extend(data.intensity_values);
            merged.frame_indices.extend(data.frame_indices);
            merged.scan_indices.extend(data.scan_indices);
        }
        
        merged
    }
}

// TimsTOFRawData structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimsTOFRawData {
    pub ms1_data: TimsTOFData,
    pub ms2_windows: Vec<((f32, f32), TimsTOFData)>,
}

// Helper structures for optimized read_timstof_data
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

// New optimized read_timstof_data function
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
    let (ms1_estimate, _ms2_estimate) = estimate_total_peaks(&frames);
    println!("  Estimated MS1 peaks: ~{}", ms1_estimate);
    
    println!("Processing frames with hybrid optimizations ({} threads)...", NUM_THREADS);
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
    
    println!("Finalizing data structures with zero-copy merge...");
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
    
    println!("\n========== Data Summary ==========");
    println!("MS1 data points: {}", global_ms1.mz_values.len());
    println!("MS2 windows: {}", ms2_vec.len());
    
    let total_ms2_points: usize = ms2_vec.iter().map(|(_, td)| td.mz_values.len()).sum();
    println!("MS2 data points: {}", total_ms2_points);
    println!("Total processing time: {:.3}s", total_start.elapsed().as_secs_f32());
    
    Ok(TimsTOFRawData {
        ms1_data: global_ms1,
        ms2_windows: ms2_vec,
    })
}

// ============================================================================
// Optimized IndexedTimsTOFData with all u32 indices
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexedTimsTOFData {
    pub rt_values_min: Vec<f32>,
    pub mobility_values: Vec<f32>,
    pub mz_values: Vec<f32>,
    pub intensity_values: Vec<u32>,
    pub frame_indices: Vec<u32>,
    pub scan_indices: Vec<u32>,
}

impl IndexedTimsTOFData {
    /// Empty constructor
    pub fn new() -> Self {
        Self {
            rt_values_min: Vec::new(),
            mobility_values: Vec::new(),
            mz_values: Vec::new(),
            intensity_values: Vec::new(),
            frame_indices: Vec::new(),
            scan_indices: Vec::new(),
        }
    }

    /// Optimized build with parallel sorting and reordering
    pub fn from_timstof_data(data: TimsTOFData) -> Self {
        let n_peaks = data.mz_values.len();
        
        // Use a threshold to decide whether to use parallel processing
        const PARALLEL_THRESHOLD: usize = 10_000;
        
        if n_peaks < PARALLEL_THRESHOLD {
            // For small datasets, use the original sequential approach
            Self::from_timstof_data_sequential(data)
        } else {
            // For large datasets, use parallel processing
            Self::from_timstof_data_parallel(data)
        }
    }
    
    /// Sequential version for small datasets
    fn from_timstof_data_sequential(data: TimsTOFData) -> Self {
        let n_peaks = data.mz_values.len();
        
        // Build permutation for m/z sorting
        let mut order: Vec<usize> = (0..n_peaks).collect();
        order.sort_by(|&a, &b| data.mz_values[a].partial_cmp(&data.mz_values[b]).unwrap());

        // Helper functions to reorder in one pass
        fn reorder_f32(src: &[f32], ord: &[usize]) -> Vec<f32> {
            ord.iter().map(|&i| src[i]).collect()
        }
        
        fn reorder_u32(src: &[u32], ord: &[usize]) -> Vec<u32> {
            ord.iter().map(|&i| src[i]).collect()
        }

        // Apply permutation to all columns
        Self {
            rt_values_min: reorder_f32(&data.rt_values_min, &order),
            mobility_values: reorder_f32(&data.mobility_values, &order),
            mz_values: reorder_f32(&data.mz_values, &order),
            intensity_values: reorder_u32(&data.intensity_values, &order),
            frame_indices: reorder_u32(&data.frame_indices, &order),
            scan_indices: reorder_u32(&data.scan_indices, &order),
        }
    }
    
    /// Parallel version for large datasets
    fn from_timstof_data_parallel(data: TimsTOFData) -> Self {
        let n_peaks = data.mz_values.len();
        
        // Step 1: Create index-value pairs for parallel sorting
        let mut indexed_mz: Vec<(usize, f32)> = (0..n_peaks)
            .into_par_iter()
            .map(|i| (i, data.mz_values[i]))
            .collect();
        
        // Step 2: Parallel sort by m/z values
        indexed_mz.par_sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        
        // Step 3: Extract the sorted order
        let order: Vec<usize> = indexed_mz.into_par_iter()
            .map(|(idx, _)| idx)
            .collect();
        
        // Step 4: Parallel reordering using rayon with correct destructuring
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
    
    /// Parallel reordering for f32 arrays
    fn reorder_f32_parallel(src: &[f32], order: &[usize]) -> Vec<f32> {
        order.par_iter()
            .map(|&i| src[i])
            .collect()
    }
    
    /// Parallel reordering for u32 arrays
    fn reorder_u32_parallel(src: &[u32], order: &[usize]) -> Vec<u32> {
        order.par_iter()
            .map(|&i| src[i])
            .collect()
    }

    /// Locate the slice boundaries (binary search)
    #[inline]
    fn range_indices(&self, mz_min: f32, mz_max: f32) -> std::ops::Range<usize> {
        let start = self.mz_values.partition_point(|&x| x < mz_min);
        let end = self.mz_values.partition_point(|&x| x <= mz_max);
        start..end
    }

    /// Extract peaks whose m/z is within [mz_min, mz_max]
    pub fn slice_by_mz_range(&self, mz_min: f32, mz_max: f32) -> TimsTOFData {
        let range = self.range_indices(mz_min, mz_max);
        let cap = range.len();
        let mut td = TimsTOFData::with_capacity(cap);

        td.rt_values_min.extend_from_slice(&self.rt_values_min[range.clone()]);
        td.mobility_values.extend_from_slice(&self.mobility_values[range.clone()]);
        td.mz_values.extend_from_slice(&self.mz_values[range.clone()]);
        td.intensity_values.extend_from_slice(&self.intensity_values[range.clone()]);
        td.frame_indices.extend_from_slice(&self.frame_indices[range.clone()]);
        td.scan_indices.extend_from_slice(&self.scan_indices[range]);
        td
    }

    /// Combined m/z and ion mobility range filtering (NEW - optimized)
    pub fn slice_by_mz_im_range(&self, mz_min: f32, mz_max: f32, im_min: f32, im_max: f32) -> TimsTOFData {
        let range = self.range_indices(mz_min, mz_max);
        
        let indices: Vec<usize> = (range.start..range.end)
            .into_par_iter()         // Use parallel filtering for ion mobility
            .filter(|&i| {
                let im = self.mobility_values[i];
                im >= im_min && im <= im_max
            })
            .collect();
        
        let cap = indices.len();
        let mut td = TimsTOFData::with_capacity(cap);
        
        // Copy only the filtered indices
        for &i in &indices {
            td.rt_values_min.push(self.rt_values_min[i]);
            td.mobility_values.push(self.mobility_values[i]);
            td.mz_values.push(self.mz_values[i]);
            td.intensity_values.push(self.intensity_values[i]);
            td.frame_indices.push(self.frame_indices[i]);
            td.scan_indices.push(self.scan_indices[i]);
        }
        
        td
    }

    /// Multiply m/z by 1000 (monotonic transform keeps sorting)
    pub fn convert_mz_to_integer(&mut self) {
        self.mz_values.iter_mut().for_each(|v| *v = (*v * 1000.0).ceil());
    }

    /// Ion mobility filtering (now uses slice_by_mz_im_range internally)
    pub fn filter_by_im_range(&self, im_min: f32, im_max: f32) -> TimsTOFData {
        // Use the full m/z range with IM filtering
        self.slice_by_mz_im_range(f32::NEG_INFINITY, f32::INFINITY, im_min, im_max)
    }
}

/// Optimized build_indexed_data with better parallelization
pub fn build_indexed_data(raw_data: TimsTOFRawData) -> Result<(IndexedTimsTOFData, Vec<((f32, f32), IndexedTimsTOFData)>), Box<dyn Error>> {
    // Check data sizes to determine parallelization strategy
    let ms1_size = raw_data.ms1_data.mz_values.len();
    let ms2_count = raw_data.ms2_windows.len();
    
    println!("Building indexed data: MS1 points={}, MS2 windows={}", ms1_size, ms2_count);
    
    // Use parallel processing for both MS1 and MS2
    let (ms1_indexed, ms2_indexed_pairs) = rayon::join(
        || {
            // Build MS1 index (will use parallel processing internally for large datasets)
            IndexedTimsTOFData::from_timstof_data(raw_data.ms1_data)
        },
        || {
            // Build MS2 indices in parallel
            if ms2_count > 1 {
                // Process MS2 windows in parallel
                raw_data.ms2_windows
                    .into_par_iter()
                    .map(|((low, high), data)| {
                        let indexed = IndexedTimsTOFData::from_timstof_data(data);
                        ((low, high), indexed)
                    })
                    .collect()
            } else {
                // For single window, process sequentially
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
// 原有的数据结构和工具函数
// ============================================================================

#[inline]
pub fn quantize(x: f32) -> u32 { 
    (x * 10_000.0).round() as u32 
}

pub struct FrameSplit {
    pub ms1: TimsTOFData,
    pub ms2: Vec<((u32, u32), TimsTOFData)>,
}

pub trait MergeFrom { 
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

// 常量定义
pub const MS1_ISOTOPE_COUNT: usize = 6;
pub const FRAGMENT_VARIANTS: usize = 3;
pub const MS1_TYPE_MARKER: f32 = 5.0;
pub const MS1_FRAGMENT_TYPE: f32 = 1.0;
pub const VARIANT_ORIGINAL: f32 = 2.0;
pub const VARIANT_LIGHT: f32 = 3.0;
pub const VARIANT_HEAVY: f32 = 4.0;

// 库列名映射结构体
#[derive(Debug, Clone)]
pub struct LibCols {
    pub precursor_mz_col: &'static str,
    pub irt_col: &'static str,
    pub precursor_id_col: &'static str,
    pub full_sequence_col: &'static str,
    pub pure_sequence_col: &'static str,
    pub precursor_charge_col: &'static str,
    pub fragment_mz_col: &'static str,
    pub fragment_series_col: &'static str,
    pub fragment_charge_col: &'static str,
    pub fragment_type_col: &'static str,
    pub lib_intensity_col: &'static str,
    pub protein_name_col: &'static str,
    pub decoy_or_not_col: &'static str,
}

impl Default for LibCols {
    fn default() -> Self {
        LibCols {
            precursor_mz_col: "PrecursorMz",
            irt_col: "Tr_recalibrated",
            precursor_id_col: "transition_group_id",
            full_sequence_col: "FullUniModPeptideName",
            pure_sequence_col: "PeptideSequence",
            precursor_charge_col: "PrecursorCharge",
            fragment_mz_col: "ProductMz",
            fragment_series_col: "FragmentNumber",
            fragment_charge_col: "FragmentCharge",
            fragment_type_col: "FragmentType",
            lib_intensity_col: "LibraryIntensity",
            protein_name_col: "ProteinName",
            decoy_or_not_col: "decoy",
        }
    }
}

pub type MSDataArray = Vec<Vec<f32>>;

#[derive(Debug, Clone)]
pub struct LibraryRecord {
    pub transition_group_id: String,
    pub peptide_sequence: String,
    pub full_unimod_peptide_name: String,
    pub precursor_charge: String,
    pub precursor_mz: String,
    pub tr_recalibrated: String,
    pub product_mz: String,
    pub fragment_type: String,
    pub fragment_charge: String,
    pub fragment_number: String,
    pub library_intensity: String,
    pub protein_id: String,
    pub protein_name: String,
    pub gene: String,
    pub decoy: String,
    pub other_columns: HashMap<String, String>,
}

pub fn find_scan_for_index(index: usize, scan_offsets: &[usize]) -> usize {
    for (scan, window) in scan_offsets.windows(2).enumerate() {
        if index >= window[0] && index < window[1] {
            return scan;
        }
    }
    scan_offsets.len() - 1
}

pub fn intercept_frags_sort(mut fragment_list: Vec<f32>, max_length: usize) -> Vec<f32> {
    fragment_list.sort_by(|a, b| b.partial_cmp(a).unwrap_or(Ordering::Equal));
    fragment_list.truncate(max_length);
    fragment_list
}

pub fn get_precursor_indices(precursor_ids: &[String]) -> Vec<Vec<usize>> {
    let mut precursor_indices = Vec::new();
    let mut current_group = Vec::new();
    let mut last_id = "";
    
    for (idx, id) in precursor_ids.iter().enumerate() {
        if idx == 0 || id == last_id {
            current_group.push(idx);
        } else {
            if !current_group.is_empty() {
                precursor_indices.push(current_group.clone());
                current_group.clear();
            }
            current_group.push(idx);
        }
        last_id = id;
    }
    
    if !current_group.is_empty() {
        precursor_indices.push(current_group);
    }
    
    precursor_indices
}

pub fn get_lib_col_dict() -> HashMap<&'static str, &'static str> {
    let mut lib_col_dict = HashMap::new();
    for key in ["transition_group_id", "PrecursorID"] { lib_col_dict.insert(key, "transition_group_id"); }
    for key in ["PeptideSequence", "Sequence", "StrippedPeptide"] { lib_col_dict.insert(key, "PeptideSequence"); }
    for key in ["FullUniModPeptideName", "ModifiedPeptide", "LabeledSequence", "modification_sequence", "ModifiedPeptideSequence"] { lib_col_dict.insert(key, "FullUniModPeptideName"); }
    for key in ["PrecursorCharge", "Charge", "prec_z"] { lib_col_dict.insert(key, "PrecursorCharge"); }
    for key in ["PrecursorMz", "Q1"] { lib_col_dict.insert(key, "PrecursorMz"); }
    for key in ["Tr_recalibrated", "iRT", "RetentionTime", "NormalizedRetentionTime", "RT_detected"] { lib_col_dict.insert(key, "Tr_recalibrated"); }
    for key in ["ProductMz", "FragmentMz", "Q3"] { lib_col_dict.insert(key, "ProductMz"); }
    for key in ["FragmentType", "FragmentIonType", "ProductType", "ProductIonType", "frg_type"] { lib_col_dict.insert(key, "FragmentType"); }
    for key in ["FragmentCharge", "FragmentIonCharge", "ProductCharge", "ProductIonCharge", "frg_z"] { lib_col_dict.insert(key, "FragmentCharge"); }
    for key in ["FragmentNumber", "frg_nr", "FragmentSeriesNumber"] { lib_col_dict.insert(key, "FragmentNumber"); }
    for key in ["LibraryIntensity", "RelativeIntensity", "RelativeFragmentIntensity", "RelativeFragmentIonIntensity", "relative_intensity"] { lib_col_dict.insert(key, "LibraryIntensity"); }
    for key in ["ProteinID", "ProteinId", "UniprotID", "uniprot_id", "UniProtIds"] { lib_col_dict.insert(key, "ProteinID"); }
    for key in ["ProteinName", "Protein Name", "Protein_name", "protein_name"] { lib_col_dict.insert(key, "ProteinName"); }
    for key in ["Gene", "Genes", "GeneName"] { lib_col_dict.insert(key, "Gene"); }
    for key in ["Decoy", "decoy"] { lib_col_dict.insert(key, "decoy"); }
    lib_col_dict
}

// Helper functions for MS data processing
pub fn build_ms1_data(fragment_list: &[Vec<f32>], isotope_range: f32, max_mz: f32) -> MSDataArray {
    let first_fragment = &fragment_list[0];
    let charge = first_fragment[1];
    let precursor_mz = first_fragment[5];
    
    let available_range = (max_mz - precursor_mz) * charge;
    let iso_shift_max = (isotope_range.min(available_range) as i32) + 1;
    
    let mut isotope_mz_list: Vec<f32> = (0..iso_shift_max)
        .map(|iso_shift| precursor_mz + (iso_shift as f32) / charge)
        .collect();
    
    isotope_mz_list = intercept_frags_sort(isotope_mz_list, MS1_ISOTOPE_COUNT);
    
    let mut ms1_data = Vec::new();
    for mz in isotope_mz_list {
        let row = vec![
            mz,
            first_fragment[1],
            first_fragment[2],
            first_fragment[3],
            3.0,
            first_fragment[5],
            MS1_TYPE_MARKER,
            0.0,
            MS1_FRAGMENT_TYPE,
        ];
        ms1_data.push(row);
    }
    
    while ms1_data.len() < MS1_ISOTOPE_COUNT {
        ms1_data.push(vec![0.0; 9]);
    }
    
    ms1_data
}

pub fn build_ms2_data(fragment_list: &[Vec<f32>], max_fragment_num: usize) -> MSDataArray {
    let total_count = max_fragment_num * FRAGMENT_VARIANTS;
    let fragment_num = fragment_list.len();
    
    let mut tripled_fragments = Vec::new();
    for _ in 0..FRAGMENT_VARIANTS {
        for fragment in fragment_list {
            tripled_fragments.push(fragment.clone());
        }
    }
    
    let total_rows = fragment_num * FRAGMENT_VARIANTS;
    
    let mut type_column = vec![0.0; total_rows];
    for i in fragment_num..(fragment_num * 2) {
        type_column[i] = -1.0;
    }
    for i in (fragment_num * 2)..total_rows {
        type_column[i] = 1.0;
    }
    
    let window_id_column = vec![0.0; total_rows];
    
    let mut variant_type_column = vec![0.0; total_rows];
    for i in 0..fragment_num {
        variant_type_column[i] = VARIANT_ORIGINAL;
    }
    for i in fragment_num..(fragment_num * 2) {
        variant_type_column[i] = VARIANT_LIGHT;
    }
    for i in (fragment_num * 2)..total_rows {
        variant_type_column[i] = VARIANT_HEAVY;
    }
    
    let mut complete_data = Vec::new();
    for i in 0..total_rows {
        let mut row = tripled_fragments[i].clone();
        row.push(type_column[i]);
        row.push(window_id_column[i]);
        row.push(variant_type_column[i]);
        complete_data.push(row);
    }
    
    if complete_data.len() >= total_count {
        complete_data.truncate(total_count);
    } else {
        let row_size = if !complete_data.is_empty() { complete_data[0].len() } else { 9 };
        while complete_data.len() < total_count {
            complete_data.push(vec![0.0; row_size]);
        }
    }
    
    complete_data
}

pub fn build_precursor_info(fragment_list: &[Vec<f32>]) -> Vec<f32> {
    let first_fragment = &fragment_list[0];
    vec![
        first_fragment[7],
        first_fragment[5],
        first_fragment[1],
        first_fragment[6],
        fragment_list.len() as f32,
        0.0,
    ]
}

pub fn format_ms_data(
    fragment_list: &[Vec<f32>], 
    isotope_range: f32, 
    max_mz: f32, 
    max_fragment: usize
) -> (MSDataArray, MSDataArray, Vec<f32>) {
    let ms1_data = build_ms1_data(fragment_list, isotope_range, max_mz);
    
    let fragment_list_subset: Vec<Vec<f32>> = fragment_list.iter()
        .map(|row| row[..6].to_vec())
        .collect();
    
    let mut ms2_data = build_ms2_data(&fragment_list_subset, max_fragment);
    
    let mut ms1_copy = ms1_data.clone();
    for row in &mut ms1_copy {
        if row.len() > 8 {
            row[8] = 5.0;
        }
    }
    
    ms2_data.extend(ms1_copy);
    
    let precursor_info = build_precursor_info(fragment_list);
    
    (ms1_data, ms2_data, precursor_info)
}

pub fn build_lib_matrix(
    lib_data: &[LibraryRecord],
    lib_cols: &LibCols,
    iso_range: f32,
    mz_max: f32,
    max_fragment: usize,
) -> Result<(Vec<Vec<String>>, Vec<MSDataArray>, Vec<MSDataArray>, Vec<Vec<f32>>), Box<dyn Error>> {
    let precursor_ids: Vec<String> = lib_data.iter()
        .map(|record| record.transition_group_id.clone())
        .collect();
    
    let precursor_groups = get_precursor_indices(&precursor_ids);
    
    let mut all_precursors = Vec::new();
    let mut all_ms1_data = Vec::new();
    let mut all_ms2_data = Vec::new();
    let mut all_precursor_info = Vec::new();
    
    for (group_idx, indices) in precursor_groups.iter().enumerate() {
        if indices.is_empty() {
            continue;
        }
        
        let first_idx = indices[0];
        let first_record = &lib_data[first_idx];
        
        let precursor_info = vec![
            first_record.transition_group_id.clone(),
            first_record.decoy.clone(),
        ];
        all_precursors.push(precursor_info);
        
        let mut group_fragments = Vec::new();
        for &idx in indices {
            let record = &lib_data[idx];
            
            let fragment_row = vec![
                record.product_mz.parse::<f32>().unwrap_or(0.0),
                record.precursor_charge.parse::<f32>().unwrap_or(0.0),
                record.fragment_charge.parse::<f32>().unwrap_or(0.0),
                record.library_intensity.parse::<f32>().unwrap_or(0.0),
                record.fragment_type.parse::<f32>().unwrap_or(0.0),
                record.precursor_mz.parse::<f32>().unwrap_or(0.0),
                record.tr_recalibrated.parse::<f32>().unwrap_or(0.0),
                record.peptide_sequence.len() as f32,
                record.decoy.parse::<f32>().unwrap_or(0.0),
                record.transition_group_id.len() as f32,
            ];
            group_fragments.push(fragment_row);
        }
        
        let (ms1, ms2, info) = format_ms_data(&group_fragments, iso_range, mz_max, max_fragment);
        
        all_ms1_data.push(ms1);
        all_ms2_data.push(ms2);
        all_precursor_info.push(info);
    }
    
    Ok((all_precursors, all_ms1_data, all_ms2_data, all_precursor_info))
}

pub fn build_precursors_matrix_step1(
    ms1_data_list: &[MSDataArray], 
    ms2_data_list: &[MSDataArray], 
    device: &str
) -> Result<(Array3<f32>, Array3<f32>), Box<dyn Error>> {
    if ms1_data_list.is_empty() || ms2_data_list.is_empty() {
        return Err("MS1或MS2数据列表为空".into());
    }
    
    let batch_size = ms1_data_list.len();
    let ms1_rows = ms1_data_list[0].len();
    let ms1_cols = if !ms1_data_list[0].is_empty() { ms1_data_list[0][0].len() } else { 0 };
    let ms2_rows = ms2_data_list[0].len();
    let ms2_cols = if !ms2_data_list[0].is_empty() { ms2_data_list[0][0].len() } else { 0 };
    
    let mut ms1_tensor = Array3::<f32>::zeros((batch_size, ms1_rows, ms1_cols));
    for (i, ms1_data) in ms1_data_list.iter().enumerate() {
        for (j, row) in ms1_data.iter().enumerate() {
            for (k, &val) in row.iter().enumerate() {
                ms1_tensor[[i, j, k]] = val;
            }
        }
    }
    
    let mut ms2_tensor = Array3::<f32>::zeros((batch_size, ms2_rows, ms2_cols));
    for (i, ms2_data) in ms2_data_list.iter().enumerate() {
        for (j, row) in ms2_data.iter().enumerate() {
            for (k, &val) in row.iter().enumerate() {
                ms2_tensor[[i, j, k]] = val;
            }
        }
    }
    
    Ok((ms1_tensor, ms2_tensor))
}

pub fn build_precursors_matrix_step2(mut ms2_data_tensor: Array3<f32>) -> Array3<f32> {
    let shape = ms2_data_tensor.shape();
    let (batch, rows, cols) = (shape[0], shape[1], shape[2]);
    
    for i in 0..batch {
        for j in 0..rows {
            if cols > 6 {
                let val0 = ms2_data_tensor[[i, j, 0]];
                let val6 = ms2_data_tensor[[i, j, 6]];
                let val2 = ms2_data_tensor[[i, j, 2]];
                
                if val2 != 0.0 {
                    ms2_data_tensor[[i, j, 0]] = val0 + val6 / val2;
                }
            }
        }
    }
    
    for i in 0..batch {
        for j in 0..rows {
            for k in 0..cols {
                let val = ms2_data_tensor[[i, j, k]];
                if val.is_infinite() || val.is_nan() {
                    ms2_data_tensor[[i, j, k]] = 0.0;
                }
            }
        }
    }
    
    ms2_data_tensor
}

pub fn extract_width_2(
    mz_to_extract: &Array3<f32>,
    mz_unit: &str,
    mz_tol: f32,
    max_extract_len: usize,
    frag_repeat_num: usize,
    max_moz_num: f32,
    device: &str
) -> Result<Array3<f32>, Box<dyn Error>> {
    let shape = mz_to_extract.shape();
    let (batch, rows, _) = (shape[0], shape[1], shape[2]);
    
    let is_all_zero = mz_to_extract.iter().all(|&v| v == 0.0);
    if is_all_zero {
        return Ok(Array3::<f32>::zeros((batch, rows, 2)));
    }
    
    let mut mz_tol_full = Array3::<f32>::zeros((batch, rows, 1));
    
    match mz_unit {
        "Da" => {
            for i in 0..batch {
                for j in 0..rows {
                    mz_tol_full[[i, j, 0]] = mz_tol;
                }
            }
        },
        "ppm" => {
            for i in 0..batch {
                for j in 0..rows {
                    mz_tol_full[[i, j, 0]] = mz_to_extract[[i, j, 0]] * mz_tol * 0.000001;
                }
            }
        },
        _ => return Err(format!("Invalid mz_unit format: {}. Only Da and ppm are supported.", mz_unit).into()),
    }
    
    for i in 0..batch {
        for j in 0..rows {
            if mz_tol_full[[i, j, 0]].is_nan() {
                mz_tol_full[[i, j, 0]] = 0.0;
            }
        }
    }
    
    let mz_tol_full_num = max_moz_num / 1000.0;
    for i in 0..batch {
        for j in 0..rows {
            if mz_tol_full[[i, j, 0]] > mz_tol_full_num {
                mz_tol_full[[i, j, 0]] = mz_tol_full_num;
            }
        }
    }
    
    for i in 0..batch {
        for j in 0..rows {
            let val = mz_tol_full[[i, j, 0]];
            mz_tol_full[[i, j, 0]] = ((val * 1000.0 / frag_repeat_num as f32).ceil()) * frag_repeat_num as f32;
        }
    }
    
    let mut extract_width_range_list = Array3::<f32>::zeros((batch, rows, 2));
    
    for i in 0..batch {
        for j in 0..rows {
            let mz_val = mz_to_extract[[i, j, 0]] * 1000.0;
            let tol_val = mz_tol_full[[i, j, 0]];
            extract_width_range_list[[i, j, 0]] = (mz_val - tol_val).floor();
            extract_width_range_list[[i, j, 1]] = (mz_val + tol_val).floor();
        }
    }
    
    Ok(extract_width_range_list)
}

pub fn build_range_matrix_step3(
    ms1_data_tensor: &Array3<f32>,
    ms2_data_tensor: &Array3<f32>,
    frag_repeat_num: usize,
    mz_unit: &str,
    mz_tol_ms1: f32,
    mz_tol_ms2: f32,
    device: &str
) -> Result<(Array3<f32>, Array3<f32>), Box<dyn Error>> {
    let shape1 = ms1_data_tensor.shape();
    let shape2 = ms2_data_tensor.shape();
    
    let mut re_ms1_data_tensor = Array3::<f32>::zeros((shape1[0], shape1[1] * frag_repeat_num, shape1[2]));
    let mut re_ms2_data_tensor = Array3::<f32>::zeros((shape2[0], shape2[1] * frag_repeat_num, shape2[2]));
    
    for i in 0..shape1[0] {
        for rep in 0..frag_repeat_num {
            for j in 0..shape1[1] {
                for k in 0..shape1[2] {
                    re_ms1_data_tensor[[i, rep * shape1[1] + j, k]] = ms1_data_tensor[[i, j, k]];
                }
            }
        }
    }
    
    for i in 0..shape2[0] {
        for rep in 0..frag_repeat_num {
            for j in 0..shape2[1] {
                for k in 0..shape2[2] {
                    re_ms2_data_tensor[[i, rep * shape2[1] + j, k]] = ms2_data_tensor[[i, j, k]];
                }
            }
        }
    }
    
    let ms1_col0 = re_ms1_data_tensor.slice(s![.., .., 0..1]).to_owned();
    let ms2_col0 = re_ms2_data_tensor.slice(s![.., .., 0..1]).to_owned();
    
    let ms1_extract_width_range_list = extract_width_2(
        &ms1_col0, mz_unit, mz_tol_ms1, 20, frag_repeat_num, 50.0, device
    )?;
    
    let ms2_extract_width_range_list = extract_width_2(
        &ms2_col0, mz_unit, mz_tol_ms2, 20, frag_repeat_num, 50.0, device
    )?;
    
    Ok((ms1_extract_width_range_list, ms2_extract_width_range_list))
}

pub fn extract_width(
    mz_to_extract: &Array3<f32>,
    mz_unit: &str,
    mz_tol: f32,
    max_extract_len: usize,
    frag_repeat_num: usize,
    max_moz_num: f32,
    device: &str
) -> Result<Array3<f32>, Box<dyn Error>> {
    let shape = mz_to_extract.shape();
    let (batch, rows, _) = (shape[0], shape[1], shape[2]);
    
    let is_all_zero = mz_to_extract.iter().all(|&v| v == 0.0);
    if is_all_zero {
        return Ok(Array3::<f32>::zeros((batch, rows, max_moz_num as usize)));
    }
    
    let mut mz_tol_half = Array3::<f32>::zeros((batch, rows, 1));
    
    match mz_unit {
        "Da" => {
            for i in 0..batch {
                for j in 0..rows {
                    mz_tol_half[[i, j, 0]] = mz_tol / 2.0;
                }
            }
        },
        "ppm" => {
            for i in 0..batch {
                for j in 0..rows {
                    mz_tol_half[[i, j, 0]] = mz_to_extract[[i, j, 0]] * mz_tol * 0.000001 / 2.0;
                }
            }
        },
        _ => return Err(format!("Invalid mz_unit format: {}. Only Da and ppm are supported.", mz_unit).into()),
    }
    
    for i in 0..batch {
        for j in 0..rows {
            if mz_tol_half[[i, j, 0]].is_nan() {
                mz_tol_half[[i, j, 0]] = 0.0;
            }
        }
    }
    
    let mz_tol_half_num = (max_moz_num / 1000.0) / 2.0;
    for i in 0..batch {
        for j in 0..rows {
            if mz_tol_half[[i, j, 0]] > mz_tol_half_num {
                mz_tol_half[[i, j, 0]] = mz_tol_half_num;
            }
        }
    }
    
    for i in 0..batch {
        for j in 0..rows {
            let val = mz_tol_half[[i, j, 0]];
            mz_tol_half[[i, j, 0]] = ((val * 1000.0 / frag_repeat_num as f32).ceil()) * frag_repeat_num as f32;
        }
    }
    
    let mut extract_width_list = Array3::<f32>::zeros((batch, rows, 2));
    
    for i in 0..batch {
        for j in 0..rows {
            let mz_val = mz_to_extract[[i, j, 0]] * 1000.0;
            let tol_val = mz_tol_half[[i, j, 0]];
            extract_width_list[[i, j, 0]] = (mz_val - tol_val).floor();
            extract_width_list[[i, j, 1]] = (mz_val + tol_val).floor();
        }
    }
    
    let batch_num = rows / frag_repeat_num;
    
    let mut cha_tensor = Array2::<f32>::zeros((batch, batch_num));
    for i in 0..batch {
        for j in 0..batch_num {
            cha_tensor[[i, j]] = (extract_width_list[[i, j, 1]] - extract_width_list[[i, j, 0]]) / frag_repeat_num as f32;
        }
    }
    
    for i in 0..batch {
        for j in 0..batch_num {
            extract_width_list[[i, j, 1]] = extract_width_list[[i, j, 0]] + cha_tensor[[i, j]] - 1.0;
        }
        
        for j in 0..batch_num {
            let idx = batch_num + j;
            if idx < rows {
                extract_width_list[[i, idx, 0]] = extract_width_list[[i, j, 0]] + cha_tensor[[i, j]];
                extract_width_list[[i, idx, 1]] = extract_width_list[[i, j, 0]] + 2.0 * cha_tensor[[i, j]] - 1.0;
            }
        }
        
        for j in 0..batch_num {
            let idx = batch_num * 2 + j;
            if idx < rows {
                extract_width_list[[i, idx, 0]] = extract_width_list[[i, j, 0]] + 2.0 * cha_tensor[[i, j]];
                extract_width_list[[i, idx, 1]] = extract_width_list[[i, j, 0]] + 3.0 * cha_tensor[[i, j]] - 1.0;
            }
        }
        
        for j in 0..batch_num {
            let idx = batch_num * 3 + j;
            if idx < rows {
                extract_width_list[[i, idx, 0]] = extract_width_list[[i, j, 0]] + 3.0 * cha_tensor[[i, j]];
                extract_width_list[[i, idx, 1]] = extract_width_list[[i, j, 0]] + 4.0 * cha_tensor[[i, j]] - 1.0;
            }
        }
        
        for j in 0..batch_num {
            let idx = batch_num * 4 + j;
            if idx < rows {
                extract_width_list[[i, idx, 0]] = extract_width_list[[i, j, 0]] + 4.0 * cha_tensor[[i, j]];
                extract_width_list[[i, idx, 1]] = extract_width_list[[i, j, 0]] + 5.0 * cha_tensor[[i, j]] - 1.0;
            }
        }
    }
    
    let mut new_tensor = Array3::<f32>::zeros((batch, rows, max_moz_num as usize));
    
    for i in 0..batch {
        for j in 0..rows {
            for k in 0..(max_moz_num as usize) {
                new_tensor[[i, j, k]] = extract_width_list[[i, j, 0]] + k as f32;
                if new_tensor[[i, j, k]] > extract_width_list[[i, j, 1]] {
                    new_tensor[[i, j, k]] = 0.0;
                }
            }
        }
    }
    
    Ok(new_tensor)
}

pub fn build_precursors_matrix_step3(
    ms1_data_tensor: &Array3<f32>,
    ms2_data_tensor: &Array3<f32>,
    frag_repeat_num: usize,
    mz_unit: &str,
    mz_tol_ms1: f32,
    mz_tol_ms2: f32,
    device: &str
) -> Result<(Array3<f32>, Array3<f32>, Array3<f32>, Array3<f32>), Box<dyn Error>> {
    let shape1 = ms1_data_tensor.shape();
    let shape2 = ms2_data_tensor.shape();
    
    let mut re_ms1_data_tensor = Array3::<f32>::zeros((shape1[0], shape1[1] * frag_repeat_num, shape1[2]));
    let mut re_ms2_data_tensor = Array3::<f32>::zeros((shape2[0], shape2[1] * frag_repeat_num, shape2[2]));
    
    for i in 0..shape1[0] {
        for rep in 0..frag_repeat_num {
            for j in 0..shape1[1] {
                for k in 0..shape1[2] {
                    re_ms1_data_tensor[[i, rep * shape1[1] + j, k]] = ms1_data_tensor[[i, j, k]];
                }
            }
        }
    }
    
    for i in 0..shape2[0] {
        for rep in 0..frag_repeat_num {
            for j in 0..shape2[1] {
                for k in 0..shape2[2] {
                    re_ms2_data_tensor[[i, rep * shape2[1] + j, k]] = ms2_data_tensor[[i, j, k]];
                }
            }
        }
    }
    
    let ms1_col0 = re_ms1_data_tensor.slice(s![.., .., 0..1]).to_owned();
    let ms2_col0 = re_ms2_data_tensor.slice(s![.., .., 0..1]).to_owned();
    
    let ms1_extract_width_range_list = extract_width(
        &ms1_col0, mz_unit, mz_tol_ms1, 20, frag_repeat_num, 50.0, device
    )?;
    
    let ms2_extract_width_range_list = extract_width(
        &ms2_col0, mz_unit, mz_tol_ms2, 20, frag_repeat_num, 50.0, device
    )?;
    
    Ok((re_ms1_data_tensor, re_ms2_data_tensor, ms1_extract_width_range_list, ms2_extract_width_range_list))
}

// Functions moved from main.rs
pub fn read_parquet_with_polars(file_path: &str) -> PolarsResult<DataFrame> {
    let file = File::open(file_path)?;
    let mut df = ParquetReader::new(file).finish()?;
    let new_col = df.column("Precursor.Id")?.clone().with_name("transition_group_id");
    df.with_column(new_col)?;
    Ok(df)
}

pub fn library_records_to_dataframe(records: Vec<LibraryRecord>) -> PolarsResult<DataFrame> {
    let mut transition_group_ids = Vec::with_capacity(records.len());
    let mut precursor_mzs = Vec::with_capacity(records.len());
    let mut product_mzs = Vec::with_capacity(records.len());
    for record in records {
        transition_group_ids.push(record.transition_group_id);
        precursor_mzs.push(record.precursor_mz.parse::<f32>().unwrap_or(f32::NAN));
        product_mzs.push(record.product_mz.parse::<f32>().unwrap_or(f32::NAN));
    }
    let df = DataFrame::new(vec![
        Series::new("transition_group_id", transition_group_ids),
        Series::new("PrecursorMz", precursor_mzs),
        Series::new("ProductMz", product_mzs),
    ])?;
    Ok(df)
}

pub fn merge_library_and_report(library_df: DataFrame, report_df: DataFrame) -> PolarsResult<DataFrame> {
    let report_selected = report_df.select(["transition_group_id", "RT", "IM", "iIM"])?;
    let merged = library_df.join(&report_selected, ["transition_group_id"], ["transition_group_id"], JoinArgs::new(JoinType::Left))?;
    let rt_col = merged.column("RT")?;
    let mask = rt_col.is_not_null();
    let filtered = merged.filter(&mask)?;
    let reordered = filtered.select(["transition_group_id", "PrecursorMz", "ProductMz", "RT", "IM", "iIM"])?;
    Ok(reordered)
}

pub fn get_unique_precursor_ids(diann_result: &DataFrame) -> PolarsResult<DataFrame> {
    let unique_df = diann_result.unique(Some(&["transition_group_id".to_string()]), UniqueKeepStrategy::First, None)?;
    let selected_df = unique_df.select(["transition_group_id", "RT", "IM"])?;
    Ok(selected_df)
}

pub fn process_library_fast(file_path: &str) -> Result<Vec<LibraryRecord>, Box<dyn Error>> {
    eprintln!("Reading library file: {}", file_path);
    let file = File::open(file_path)?;
    let mut reader = ReaderBuilder::new()
        .delimiter(b'\t')
        .has_headers(true)
        .from_reader(file);
    
    let headers = reader.headers()?.clone();
    let mut column_indices = HashMap::new();
    for (i, header) in headers.iter().enumerate() {
        column_indices.insert(header, i);
    }
    
    // Get library column mapping
    let lib_col_dict = get_lib_col_dict();
    let mut mapped_indices: HashMap<&str, usize> = HashMap::new();
    for (old_col, new_col) in &lib_col_dict {
        if let Some(&idx) = column_indices.get(old_col) {
            mapped_indices.insert(new_col, idx);
        }
    }
    
    let fragment_number_idx = column_indices.get("FragmentNumber").copied();
    
    // Read all records into memory first
    let mut byte_records = Vec::new();
    for result in reader.byte_records() {
        byte_records.push(result?);
    }
    
    eprintln!("Processing {} library records...", byte_records.len());
    
    // Process records in parallel
    let records: Vec<LibraryRecord> = byte_records.par_iter().map(|record| {
        let mut rec = LibraryRecord {
            transition_group_id: String::new(),
            peptide_sequence: String::new(),
            full_unimod_peptide_name: String::new(),
            precursor_charge: String::new(),
            precursor_mz: String::new(),
            tr_recalibrated: String::new(),
            product_mz: String::new(),
            fragment_type: String::new(),
            fragment_charge: String::new(),
            fragment_number: String::new(),
            library_intensity: String::new(),
            protein_id: String::new(),
            protein_name: String::new(),
            gene: String::new(),
            decoy: "0".to_string(),
            other_columns: HashMap::new(),
        };
        
        // Fill fields from mapped columns
        if let Some(&idx) = mapped_indices.get("PeptideSequence") { 
            if let Some(val) = record.get(idx) { 
                rec.peptide_sequence = String::from_utf8_lossy(val).into_owned(); 
            } 
        }
        if let Some(&idx) = mapped_indices.get("FullUniModPeptideName") { 
            if let Some(val) = record.get(idx) { 
                rec.full_unimod_peptide_name = String::from_utf8_lossy(val).into_owned(); 
            } 
        }
        if let Some(&idx) = mapped_indices.get("PrecursorCharge") { 
            if let Some(val) = record.get(idx) { 
                rec.precursor_charge = String::from_utf8_lossy(val).into_owned(); 
            } 
        }
        if let Some(&idx) = mapped_indices.get("PrecursorMz") { 
            if let Some(val) = record.get(idx) { 
                rec.precursor_mz = String::from_utf8_lossy(val).into_owned(); 
            } 
        }
        if let Some(&idx) = mapped_indices.get("ProductMz") { 
            if let Some(val) = record.get(idx) { 
                rec.product_mz = String::from_utf8_lossy(val).into_owned(); 
            } 
        }
        if let Some(&idx) = mapped_indices.get("FragmentType") {
            if let Some(val) = record.get(idx) {
                let fragment_str = String::from_utf8_lossy(val);
                rec.fragment_type = match fragment_str.as_ref() { 
                    "b" => "1".to_string(), 
                    "y" => "2".to_string(), 
                    "p" => "3".to_string(), 
                    _ => fragment_str.into_owned() 
                };
            }
        }
        if let Some(&idx) = mapped_indices.get("FragmentCharge") { 
            if let Some(val) = record.get(idx) { 
                rec.fragment_charge = String::from_utf8_lossy(val).into_owned(); 
            } 
        }
        if let Some(&idx) = mapped_indices.get("LibraryIntensity") { 
            if let Some(val) = record.get(idx) { 
                rec.library_intensity = String::from_utf8_lossy(val).into_owned(); 
            } 
        }
        if let Some(&idx) = mapped_indices.get("Tr_recalibrated") { 
            if let Some(val) = record.get(idx) { 
                rec.tr_recalibrated = String::from_utf8_lossy(val).into_owned(); 
            } 
        }
        if let Some(&idx) = mapped_indices.get("ProteinID") { 
            if let Some(val) = record.get(idx) { 
                rec.protein_id = String::from_utf8_lossy(val).into_owned(); 
            } 
        }
        if let Some(&idx) = mapped_indices.get("Gene") { 
            if let Some(val) = record.get(idx) { 
                rec.gene = String::from_utf8_lossy(val).into_owned(); 
            } 
        }
        if let Some(&idx) = mapped_indices.get("ProteinName") { 
            if let Some(val) = record.get(idx) { 
                rec.protein_name = String::from_utf8_lossy(val).into_owned(); 
            } 
        }
        
        if let Some(idx) = fragment_number_idx {
            if let Some(val) = record.get(idx) {
                rec.fragment_number = String::from_utf8_lossy(val).into_owned();
            }
        }
        
        // Generate transition_group_id
        rec.transition_group_id = format!("{}{}", rec.full_unimod_peptide_name, rec.precursor_charge);
        rec
    }).collect();
    
    Ok(records)
}

pub fn create_rt_im_dicts(df: &DataFrame) -> PolarsResult<(HashMap<String, f32>, HashMap<String, f32>)> {
    let id_col = df.column("transition_group_id")?;
    let id_vec = id_col.str()?.into_iter()
        .map(|opt| opt.unwrap_or("").to_string())
        .collect::<Vec<String>>();
    
    let rt_col = df.column("RT")?;
    let rt_vec: Vec<f32> = match rt_col.dtype() {
        DataType::Float32 => rt_col.f32()?.into_iter()
            .map(|opt| opt.unwrap_or(f32::NAN))
            .collect(),
        DataType::Float64 => rt_col.f64()?.into_iter()
            .map(|opt| opt.map(|v| v as f32).unwrap_or(f32::NAN))
            .collect(),
        _ => return Err(PolarsError::SchemaMismatch(
            format!("RT column type is not float: {:?}", rt_col.dtype()).into()
        )),
    };
    
    let im_col = df.column("IM")?;
    let im_vec: Vec<f32> = match im_col.dtype() {
        DataType::Float32 => im_col.f32()?.into_iter()
            .map(|opt| opt.unwrap_or(f32::NAN))
            .collect(),
        DataType::Float64 => im_col.f64()?.into_iter()
            .map(|opt| opt.map(|v| v as f32).unwrap_or(f32::NAN))
            .collect(),
        _ => return Err(PolarsError::SchemaMismatch(
            format!("IM column type is not float: {:?}", im_col.dtype()).into()
        )),
    };
    
    let mut rt_dict = HashMap::new();
    let mut im_dict = HashMap::new();
    
    for ((id, rt), im) in id_vec.iter().zip(rt_vec.iter()).zip(im_vec.iter()) {
        rt_dict.insert(id.clone(), *rt);
        im_dict.insert(id.clone(), *im);
    }
    
    Ok((rt_dict, im_dict))
}

pub fn get_rt_list(mut lst: Vec<f32>, target: f32) -> Vec<f32> {
    lst.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    
    if lst.is_empty() {
        return vec![0.0; 48];
    }
    
    if lst.len() <= 48 {
        let mut result = lst;
        result.resize(48, 0.0);
        return result;
    }
    
    let closest_idx = lst.iter()
        .enumerate()
        .min_by_key(|(_, &val)| ((val - target).abs() * 1e9) as i32)
        .map(|(idx, _)| idx)
        .unwrap_or(0);
    
    let start = if closest_idx >= 24 {
        (closest_idx - 24).min(lst.len() - 48)
    } else {
        0
    };
    
    lst[start..start + 48].to_vec()
}

pub fn build_ext_ms1_matrix(ms1_data_tensor: &Array3<f32>, device: &str) -> Array3<f32> {
    let shape = ms1_data_tensor.shape();
    let (batch, rows, _) = (shape[0], shape[1], shape[2]);
    
    let mut ext_matrix = Array3::<f32>::zeros((batch, rows, 4));
    
    for i in 0..batch {
        for j in 0..rows {
            ext_matrix[[i, j, 0]] = ms1_data_tensor[[i, j, 0]];
            if shape[2] > 3 {
                ext_matrix[[i, j, 1]] = ms1_data_tensor[[i, j, 3]];
            }
            if shape[2] > 8 {
                ext_matrix[[i, j, 2]] = ms1_data_tensor[[i, j, 8]];
            }
            if shape[2] > 4 {
                ext_matrix[[i, j, 3]] = ms1_data_tensor[[i, j, 4]];
            }
        }
    }
    
    ext_matrix
}

pub fn build_ext_ms2_matrix(ms2_data_tensor: &Array3<f32>, device: &str) -> Array3<f32> {
    let shape = ms2_data_tensor.shape();
    let (batch, rows, _) = (shape[0], shape[1], shape[2]);
    
    let mut ext_matrix = Array3::<f32>::zeros((batch, rows, 4));
    
    for i in 0..batch {
        for j in 0..rows {
            ext_matrix[[i, j, 0]] = ms2_data_tensor[[i, j, 0]];
            if shape[2] > 3 {
                ext_matrix[[i, j, 1]] = ms2_data_tensor[[i, j, 3]];
            }
            if shape[2] > 8 {
                ext_matrix[[i, j, 2]] = ms2_data_tensor[[i, j, 8]];
            }
            if shape[2] > 4 {
                ext_matrix[[i, j, 3]] = ms2_data_tensor[[i, j, 4]];
            }
        }
    }
    
    ext_matrix
}

pub fn build_frag_info(
    ms1_data_tensor: &Array3<f32>,
    ms2_data_tensor: &Array3<f32>,
    frag_repeat_num: usize,
    device: &str
) -> Array3<f32> {
    let ext_ms1_precursors_frag_rt_matrix = build_ext_ms1_matrix(ms1_data_tensor, device);
    let ext_ms2_precursors_frag_rt_matrix = build_ext_ms2_matrix(ms2_data_tensor, device);
    
    let ms1_shape = ext_ms1_precursors_frag_rt_matrix.shape().to_vec();
    let ms2_shape = ext_ms2_precursors_frag_rt_matrix.shape().to_vec();
    
    let batch = ms1_shape[0];
    let ms1_rows = ms1_shape[1];
    let ms2_rows = ms2_shape[1];
    
    let orig_ms1_shape = ms1_data_tensor.shape();
    let orig_ms2_shape = ms2_data_tensor.shape();
    let ms1_frag_count = orig_ms1_shape[1];
    let ms2_frag_count = orig_ms2_shape[1];
    
    let total_frag_count = ms1_frag_count + ms2_frag_count;
    let mut frag_info = Array3::<f32>::zeros((batch, total_frag_count, 4));
    
    for i in 0..batch {
        for j in 0..ms1_frag_count {
            for k in 0..4 {
                frag_info[[i, j, k]] = ext_ms1_precursors_frag_rt_matrix[[i, j, k]];
            }
        }
        
        for j in 0..ms2_frag_count {
            for k in 0..4 {
                frag_info[[i, ms1_frag_count + j, k]] = ext_ms2_precursors_frag_rt_matrix[[i, j, k]];
            }
        }
    }
    
    frag_info
}