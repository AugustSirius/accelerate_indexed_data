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
use crossbeam_utils::CachePadded;
use ahash::AHashMap;
use thread_local::ThreadLocal;

const NUM_THREADS: usize = 64;
const CHUNK_SIZE: usize = 128; // Process frames in chunks for better cache locality
const PREFETCH_DISTANCE: usize = 8; // Prefetch frames ahead

// ============================================================================
// Core Data Structures with Better Memory Layout
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(C)]
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
        // Align to cache line size (64 bytes = 16 floats)
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
        // Force allocation by writing zeros
        unsafe {
            data.rt_values_min.set_len(capacity);
            data.mobility_values.set_len(capacity);
            data.mz_values.set_len(capacity);
            data.intensity_values.set_len(capacity);
            data.frame_indices.set_len(capacity);
            data.scan_indices.set_len(capacity);
            
            // Zero-initialize to force page allocation
            std::ptr::write_bytes(data.rt_values_min.as_mut_ptr(), 0, capacity);
            std::ptr::write_bytes(data.mobility_values.as_mut_ptr(), 0, capacity);
            std::ptr::write_bytes(data.mz_values.as_mut_ptr(), 0, capacity);
            std::ptr::write_bytes(data.intensity_values.as_mut_ptr(), 0, capacity);
            std::ptr::write_bytes(data.frame_indices.as_mut_ptr(), 0, capacity);
            std::ptr::write_bytes(data.scan_indices.as_mut_ptr(), 0, capacity);
            
            // Reset length
            data.rt_values_min.set_len(0);
            data.mobility_values.set_len(0);
            data.mz_values.set_len(0);
            data.intensity_values.set_len(0);
            data.frame_indices.set_len(0);
            data.scan_indices.set_len(0);
        }
        data
    }
    
    #[inline(always)]
    unsafe fn append_at_offset(&mut self, other: &TimsTOFData, offset: usize) {
        let len = other.rt_values_min.len();
        
        std::ptr::copy_nonoverlapping(
            other.rt_values_min.as_ptr(),
            self.rt_values_min.as_mut_ptr().add(offset),
            len
        );
        std::ptr::copy_nonoverlapping(
            other.mobility_values.as_ptr(),
            self.mobility_values.as_mut_ptr().add(offset),
            len
        );
        std::ptr::copy_nonoverlapping(
            other.mz_values.as_ptr(),
            self.mz_values.as_mut_ptr().add(offset),
            len
        );
        std::ptr::copy_nonoverlapping(
            other.intensity_values.as_ptr(),
            self.intensity_values.as_mut_ptr().add(offset),
            len
        );
        std::ptr::copy_nonoverlapping(
            other.frame_indices.as_ptr(),
            self.frame_indices.as_mut_ptr().add(offset),
            len
        );
        std::ptr::copy_nonoverlapping(
            other.scan_indices.as_ptr(),
            self.scan_indices.as_mut_ptr().add(offset),
            len
        );
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
// Thread-Local Accumulator for Lock-Free Processing
// ============================================================================

struct ThreadLocalAccumulator {
    ms1_data: TimsTOFData,
    ms2_data: AHashMap<(u32, u32), TimsTOFData>,
    capacity_per_thread: usize,
}

impl ThreadLocalAccumulator {
    fn new(estimated_ms1_per_thread: usize, estimated_ms2_windows: usize) -> Self {
        Self {
            ms1_data: TimsTOFData::with_capacity(estimated_ms1_per_thread),
            ms2_data: AHashMap::with_capacity(estimated_ms2_windows),
            capacity_per_thread: estimated_ms1_per_thread,
        }
    }
    
    fn add_ms1(&mut self, data: TimsTOFData) {
        self.ms1_data.rt_values_min.extend(&data.rt_values_min);
        self.ms1_data.mobility_values.extend(&data.mobility_values);
        self.ms1_data.mz_values.extend(&data.mz_values);
        self.ms1_data.intensity_values.extend(&data.intensity_values);
        self.ms1_data.frame_indices.extend(&data.frame_indices);
        self.ms1_data.scan_indices.extend(&data.scan_indices);
    }
    
    fn add_ms2(&mut self, key: (u32, u32), data: TimsTOFData) {
        let entry = self.ms2_data.entry(key).or_insert_with(|| {
            TimsTOFData::with_capacity(1000)
        });
        entry.rt_values_min.extend(&data.rt_values_min);
        entry.mobility_values.extend(&data.mobility_values);
        entry.mz_values.extend(&data.mz_values);
        entry.intensity_values.extend(&data.intensity_values);
        entry.frame_indices.extend(&data.frame_indices);
        entry.scan_indices.extend(&data.scan_indices);
    }
}

// ============================================================================
// Optimized Frame Processor with SIMD-friendly operations
// ============================================================================

struct FrameProcessor {
    mz_cv: Arc<Tof2MzConverter>,
    im_cv: Arc<Scan2ImConverter>,
}

impl FrameProcessor {
    #[inline(always)]
    fn process_peaks_vectorized(
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
        
        // Pre-compute scan indices for all peaks
        let mut scan_indices = Vec::with_capacity(n_peaks);
        for p_idx in 0..n_peaks {
            scan_indices.push(find_scan_for_index_binary(p_idx, scan_offsets));
        }
        
        // Process in batches for better vectorization
        const BATCH: usize = 64;
        let mut i = 0;
        
        while i < n_peaks {
            let end = std::cmp::min(i + BATCH, n_peaks);
            
            for j in i..end {
                let scan = scan_indices[j];
                
                if let Some((start, end)) = scan_filter {
                    if scan < start || scan > end { continue; }
                }
                
                let tof = tof_indices[j];
                let intensity = intensities[j];
                
                // Convert in batch-friendly way
                let mz = self.mz_cv.convert(tof as f64) as f32;
                let im = self.im_cv.convert(scan as f64) as f32;
                
                data.rt_values_min.push(rt_min);
                data.mobility_values.push(im);
                data.mz_values.push(mz);
                data.intensity_values.push(intensity);
                data.frame_indices.push(frame_index);
                data.scan_indices.push(scan as u32);
            }
            
            i = end;
        }
        
        data
    }
}

// ============================================================================
// Optimized Data Loading with Thread-Local Storage
// ============================================================================

fn estimate_total_peaks_parallel(frames: &Arc<FrameReader>) -> (usize, usize) {
    let sample_size = std::cmp::min(200, frames.len());
    
    let (ms1_sum, ms2_sum, ms1_count, ms2_count) = (0..sample_size)
        .into_par_iter()
        .map(|idx| {
            if let Ok(frame) = frames.get(idx) {
                match frame.ms_level {
                    MSLevel::MS1 => (frame.tof_indices.len(), 0, 1, 0),
                    MSLevel::MS2 => (0, frame.tof_indices.len(), 0, 1),
                    _ => (0, 0, 0, 0),
                }
            } else {
                (0, 0, 0, 0)
            }
        })
        .reduce(|| (0, 0, 0, 0), |a, b| (a.0 + b.0, a.1 + b.1, a.2 + b.2, a.3 + b.3));
    
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
    let (ms1_estimate, ms2_estimate) = estimate_total_peaks_parallel(&frames);
    println!("  Estimated MS1 peaks: ~{}", ms1_estimate);
    println!("  Estimated MS2 peaks: ~{}", ms2_estimate);
    
    // Pre-allocate with extra capacity to avoid reallocation
    let ms1_capacity = (ms1_estimate as f64 * 1.2) as usize;
    let ms2_windows_estimate = 100; // Typical number of MS2 windows
    
    println!("Processing frames with {} threads...", NUM_THREADS);
    let process_start = Instant::now();
    
    // Thread-local storage for accumulation
    let thread_local_storage = Arc::new(ThreadLocal::new());
    let processed_count = Arc::new(CachePadded::new(AtomicUsize::new(0)));
    
    // Process frames in chunks for better cache locality
    let chunk_indices: Vec<_> = (0..n_frames)
        .collect::<Vec<_>>()
        .chunks(CHUNK_SIZE)
        .map(|chunk| chunk.to_vec())
        .collect();
    
    chunk_indices.par_iter().for_each(|chunk| {
        let local = thread_local_storage.get_or(|| {
            ThreadLocalAccumulator::new(
                ms1_capacity / NUM_THREADS,
                ms2_windows_estimate
            )
        });
        
        let processor = FrameProcessor {
            mz_cv: Arc::clone(&mz_cv),
            im_cv: Arc::clone(&im_cv),
        };
        
        for &idx in chunk {
            // Prefetch next frames
            if idx + PREFETCH_DISTANCE < n_frames {
                let _ = frames.get(idx + PREFETCH_DISTANCE);
            }
            
            let frame = match frames.get(idx) {
                Ok(f) => f,
                Err(_) => continue,
            };
            
            let rt_min = frame.rt_in_seconds as f32 / 60.0;
            
            match frame.ms_level {
                MSLevel::MS1 => {
                    let ms1 = processor.process_peaks_vectorized(
                        &frame.tof_indices,
                        &frame.intensities,
                        &frame.scan_offsets,
                        rt_min,
                        frame.index as u32,
                        None,
                    );
                    
                    if !ms1.mz_values.is_empty() {
                        unsafe {
                            let local_mut = &mut *(local as *const ThreadLocalAccumulator as *mut ThreadLocalAccumulator);
                            local_mut.add_ms1(ms1);
                        }
                    }
                }
                MSLevel::MS2 => {
                    let qs = &frame.quadrupole_settings;
                    
                    for win in 0..qs.isolation_mz.len() {
                        if win >= qs.isolation_width.len() { break; }
                        
                        let prec_mz = qs.isolation_mz[win] as f32;
                        let width = qs.isolation_width[win] as f32;
                        let low = prec_mz - width * 0.5;
                        let high = prec_mz + width * 0.5;
                        let key = (quantize(low), quantize(high));
                        
                        let td = processor.process_peaks_vectorized(
                            &frame.tof_indices,
                            &frame.intensities,
                            &frame.scan_offsets,
                            rt_min,
                            frame.index as u32,
                            Some((qs.scan_starts[win], qs.scan_ends[win])),
                        );
                        
                        if !td.mz_values.is_empty() {
                            unsafe {
                                let local_mut = &mut *(local as *const ThreadLocalAccumulator as *mut ThreadLocalAccumulator);
                                local_mut.add_ms2(key, td);
                            }
                        }
                    }
                }
                _ => {}
            }
            
            processed_count.fetch_add(1, AtomicOrdering::Relaxed);
        }
    });
    
    println!("  Frame processing: {:.3}s", process_start.elapsed().as_secs_f32());
    println!("  Frames processed: {}", processed_count.load(AtomicOrdering::Relaxed));
    
    println!("Merging thread-local data...");
    let merge_start = Instant::now();
    
    // Collect all thread-local accumulators
    let accumulators: Vec<_> = Arc::try_unwrap(thread_local_storage)
        .unwrap()
        .into_iter()
        .collect();
    
    // Calculate total sizes for pre-allocation
    let total_ms1_size: usize = accumulators.iter()
        .map(|acc| acc.ms1_data.mz_values.len())
        .sum();
    
    // Pre-allocate final MS1 data structure
    let mut global_ms1 = TimsTOFData::preallocate_exact(total_ms1_size);
    
    // Parallel merge of MS1 data using unsafe operations
    let mut offset = 0;
    for acc in &accumulators {
        let len = acc.ms1_data.mz_values.len();
        if len > 0 {
            unsafe {
                global_ms1.append_at_offset(&acc.ms1_data, offset);
            }
            offset += len;
        }
    }
    
    // Set final length
    unsafe {
        global_ms1.rt_values_min.set_len(total_ms1_size);
        global_ms1.mobility_values.set_len(total_ms1_size);
        global_ms1.mz_values.set_len(total_ms1_size);
        global_ms1.intensity_values.set_len(total_ms1_size);
        global_ms1.frame_indices.set_len(total_ms1_size);
        global_ms1.scan_indices.set_len(total_ms1_size);
    }
    
    // Merge MS2 data
    let mut global_ms2_map: AHashMap<(u32, u32), Vec<TimsTOFData>> = AHashMap::new();
    
    for acc in accumulators {
        for (key, data) in acc.ms2_data {
            global_ms2_map.entry(key)
                .or_insert_with(Vec::new)
                .push(data);
        }
    }
    
    // Parallel final merge of MS2 windows
    let ms2_vec: Vec<_> = global_ms2_map
        .into_par_iter()
        .map(|((q_low, q_high), chunks)| {
            let low = q_low as f32 / 10_000.0;
            let high = q_high as f32 / 10_000.0;
            
            // Calculate total size
            let total_size: usize = chunks.iter()
                .map(|c| c.mz_values.len())
                .sum();
            
            let mut merged = TimsTOFData::preallocate_exact(total_size);
            let mut offset = 0;
            
            for chunk in &chunks {
                let len = chunk.mz_values.len();
                if len > 0 {
                    unsafe {
                        merged.append_at_offset(chunk, offset);
                    }
                    offset += len;
                }
            }
            
            unsafe {
                merged.rt_values_min.set_len(total_size);
                merged.mobility_values.set_len(total_size);
                merged.mz_values.set_len(total_size);
                merged.intensity_values.set_len(total_size);
                merged.frame_indices.set_len(total_size);
                merged.scan_indices.set_len(total_size);
            }
            
            ((low, high), merged)
        })
        .collect();
    
    println!("  Data merging: {:.3}s", merge_start.elapsed().as_secs_f32());
    println!("Total loading time: {:.3}s", total_start.elapsed().as_secs_f32());
    
    Ok(TimsTOFRawData {
        ms1_data: global_ms1,
        ms2_windows: ms2_vec,
    })
}

// ============================================================================
// Optimized Indexing with Radix Sort
// ============================================================================

impl IndexedTimsTOFData {
    pub fn from_timstof_data(data: TimsTOFData) -> Self {
        let n_peaks = data.mz_values.len();
        
        const PARALLEL_THRESHOLD: usize = 100_000;
        
        if n_peaks < PARALLEL_THRESHOLD {
            Self::from_timstof_data_parallel_optimized(data)
        } else {
            // For very large datasets, use radix sort
            Self::from_timstof_data_radix_sort(data)
        }
    }
    
    fn from_timstof_data_parallel_optimized(data: TimsTOFData) -> Self {
        let n_peaks = data.mz_values.len();
        
        // Create indices with cache-friendly access pattern
        let mut indexed_mz: Vec<(usize, u32)> = (0..n_peaks)
            .into_par_iter()
            .map(|i| {
                // Convert to integer for faster comparison
                let mz_int = (data.mz_values[i] * 10000.0) as u32;
                (i, mz_int)
            })
            .collect();
        
        // Parallel sort with integer comparison (faster than float)
        indexed_mz.par_sort_unstable_by_key(|&(_, mz)| mz);
        
        let order: Vec<usize> = indexed_mz.into_par_iter()
            .map(|(idx, _)| idx)
            .collect();
        
        // Parallel reordering with better memory access pattern
        let chunk_size = 65536; // L2 cache-friendly chunk size
        
        let rt_values_min = Self::reorder_chunked(&data.rt_values_min, &order, chunk_size);
        let mobility_values = Self::reorder_chunked(&data.mobility_values, &order, chunk_size);
        let mz_values = Self::reorder_chunked(&data.mz_values, &order, chunk_size);
        let intensity_values = Self::reorder_chunked_u32(&data.intensity_values, &order, chunk_size);
        let frame_indices = Self::reorder_chunked_u32(&data.frame_indices, &order, chunk_size);
        let scan_indices = Self::reorder_chunked_u32(&data.scan_indices, &order, chunk_size);
        
        Self {
            rt_values_min,
            mobility_values,
            mz_values,
            intensity_values,
            frame_indices,
            scan_indices,
        }
    }
    
    fn from_timstof_data_radix_sort(data: TimsTOFData) -> Self {
        let n_peaks = data.mz_values.len();
        
        // Convert mz to integers for radix sort
        let mz_ints: Vec<u32> = data.mz_values.par_iter()
            .map(|&mz| (mz * 10000.0) as u32)
            .collect();
        
        // Perform radix sort to get ordering
        let order = Self::radix_sort_indices(&mz_ints);
        
        // Parallel reordering
        let rt_values_min = Self::reorder_direct(&data.rt_values_min, &order);
        let mobility_values = Self::reorder_direct(&data.mobility_values, &order);
        let mz_values = Self::reorder_direct(&data.mz_values, &order);
        let intensity_values = Self::reorder_direct_u32(&data.intensity_values, &order);
        let frame_indices = Self::reorder_direct_u32(&data.frame_indices, &order);
        let scan_indices = Self::reorder_direct_u32(&data.scan_indices, &order);
        
        Self {
            rt_values_min,
            mobility_values,
            mz_values,
            intensity_values,
            frame_indices,
            scan_indices,
        }
    }
    
    fn radix_sort_indices(values: &[u32]) -> Vec<usize> {
        let n = values.len();
        let mut indices: Vec<usize> = (0..n).collect();
        let mut temp_indices = vec![0; n];
        
        // 32-bit radix sort with 8-bit radix
        for shift in (0..32).step_by(8) {
            let mut counts = vec![0; 256];
            
            // Count occurrences
            for &idx in &indices {
                let byte = ((values[idx] >> shift) & 0xFF) as usize;
                counts[byte] += 1;
            }
            
            // Compute cumulative counts
            let mut cumulative = 0;
            for count in &mut counts {
                let temp = *count;
                *count = cumulative;
                cumulative += temp;
            }
            
            // Place elements
            for &idx in &indices {
                let byte = ((values[idx] >> shift) & 0xFF) as usize;
                temp_indices[counts[byte]] = idx;
                counts[byte] += 1;
            }
            
            std::mem::swap(&mut indices, &mut temp_indices);
        }
        
        indices
    }
    
    fn reorder_chunked(src: &[f32], order: &[usize], chunk_size: usize) -> Vec<f32> {
        let n = src.len();
        let mut dst = vec![0.0f32; n];
        
        order.par_chunks(chunk_size)
            .zip(dst.par_chunks_mut(chunk_size))
            .for_each(|(ord_chunk, dst_chunk)| {
                for (i, &idx) in ord_chunk.iter().enumerate() {
                    dst_chunk[i] = src[idx];
                }
            });
        
        dst
    }
    
    fn reorder_chunked_u32(src: &[u32], order: &[usize], chunk_size: usize) -> Vec<u32> {
        let n = src.len();
        let mut dst = vec![0u32; n];
        
        order.par_chunks(chunk_size)
            .zip(dst.par_chunks_mut(chunk_size))
            .for_each(|(ord_chunk, dst_chunk)| {
                for (i, &idx) in ord_chunk.iter().enumerate() {
                    dst_chunk[i] = src[idx];
                }
            });
        
        dst
    }
    
    fn reorder_direct(src: &[f32], order: &[usize]) -> Vec<f32> {
        order.par_iter().map(|&i| src[i]).collect()
    }
    
    fn reorder_direct_u32(src: &[u32], order: &[usize]) -> Vec<u32> {
        order.par_iter().map(|&i| src[i]).collect()
    }
}

pub fn build_indexed_data(raw_data: TimsTOFRawData) -> Result<(IndexedTimsTOFData, Vec<((f32, f32), IndexedTimsTOFData)>), Box<dyn Error>> {
    let ms1_size = raw_data.ms1_data.mz_values.len();
    let ms2_count = raw_data.ms2_windows.len();
    
    println!("Building indexed data: MS1 points={}, MS2 windows={}", ms1_size, ms2_count);
    
    let index_start = Instant::now();
    
    // Process MS1 and MS2 in parallel
    let (ms1_indexed, ms2_indexed_pairs) = rayon::join(
        || IndexedTimsTOFData::from_timstof_data(raw_data.ms1_data),
        || {
            raw_data.ms2_windows
                .into_par_iter()
                .map(|((low, high), data)| {
                    let indexed = IndexedTimsTOFData::from_timstof_data(data);
                    ((low, high), indexed)
                })
                .collect()
        }
    );
    
    println!("  Indexing completed in: {:.3}s", index_start.elapsed().as_secs_f32());
    
    Ok((ms1_indexed, ms2_indexed_pairs))
}

// ============================================================================
// Optimized Helper Functions
// ============================================================================

#[inline(always)]
pub fn quantize(x: f32) -> u32 { 
    (x * 10_000.0).round() as u32 
}

// Binary search for better performance
#[inline(always)]
pub fn find_scan_for_index_binary(index: usize, scan_offsets: &[usize]) -> usize {
    if scan_offsets.len() <= 2 {
        return 0;
    }
    
    let mut left = 0;
    let mut right = scan_offsets.len() - 1;
    
    while left < right {
        let mid = (left + right + 1) / 2;
        if scan_offsets[mid] <= index {
            left = mid;
        } else {
            right = mid - 1;
        }
    }
    
    left
}

pub fn find_scan_for_index(index: usize, scan_offsets: &[usize]) -> usize {
    find_scan_for_index_binary(index, scan_offsets)
}

// ============================================================================
// Main
// ============================================================================

fn main() -> Result<(), Box<dyn Error>> {
    // Initialize thread pool with custom configuration
    rayon::ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .stack_size(8 * 1024 * 1024) // 8MB stack per thread
        .build_global()
        .unwrap();
    
    // Enable huge pages if available (Linux)
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        let _ = fs::write("/proc/self/nr_hugepages", "1000");
    }
    
    let args: Vec<String> = env::args().collect();
    
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
    
    println!("\n========== TIMSTOF DATA LOADER (OPTIMIZED) ==========");
    println!("Data folder: {}", d_folder);
    println!("Threads: {}", NUM_THREADS);
    println!("Memory: 400GB available");
    
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
    println!("Throughput: {:.2}M peaks/sec", 
        (ms1_indexed.mz_values.len() + total_ms2) as f32 / (load_time + index_time) / 1_000_000.0);
    
    Ok(())
}