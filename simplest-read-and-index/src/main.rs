use std::{error::Error, path::Path, time::Instant, env};
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
}

#[derive(Debug, Clone)]
pub struct IndexedTimsTOFData {
    pub rt_values_min: Vec<f32>,
    pub mobility_values: Vec<f32>,
    pub mz_values: Vec<f32>,
    pub intensity_values: Vec<u32>,
    pub frame_indices: Vec<u32>,
    pub scan_indices: Vec<u32>,
}

impl IndexedTimsTOFData {
    /// Build from TimsTOFData with m/z-ascending order
    pub fn from_timstof_data(data: TimsTOFData) -> Self {
        let n_peaks = data.mz_values.len();
        
        // Build permutation for m/z sorting
        let mut order: Vec<usize> = (0..n_peaks).collect();
        order.sort_by(|&a, &b| data.mz_values[a].partial_cmp(&data.mz_values[b]).unwrap());

        // Helper functions to reorder
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
// Core Data Reading Function
// ============================================================================

fn read_timstof_data(d_folder: &Path) -> Result<TimsTOFRawData, Box<dyn Error>> {
    println!("Reading TimsTOF data from: {:?}", d_folder);
    let tdf_path = d_folder.join("analysis.tdf");
    let meta = MetadataReader::new(&tdf_path)?;
    let mz_cv = Arc::new(meta.mz_converter);
    let im_cv = Arc::new(meta.im_converter);
    
    let frames = FrameReader::new(d_folder)?;
    let n_frames = frames.len();
    println!("  Total frames to process: {}", n_frames);
    
    let splits: Vec<FrameSplit> = (0..n_frames).into_par_iter().map(|idx| {
        let frame = frames.get(idx).expect("frame read");
        let rt_min = frame.rt_in_seconds as f32 / 60.0;
        let mut ms1 = TimsTOFData::new();
        let mut ms2_pairs: Vec<((u32,u32), TimsTOFData)> = Vec::new();
        
        match frame.ms_level {
            MSLevel::MS1 => {
                let n_peaks = frame.tof_indices.len();
                ms1 = TimsTOFData::with_capacity(n_peaks);
                for (p_idx, (&tof, &intensity)) in frame.tof_indices.iter().zip(frame.intensities.iter()).enumerate() {
                    let mz = mz_cv.convert(tof as f64) as f32;
                    let scan = find_scan_for_index(p_idx, &frame.scan_offsets);
                    let im = im_cv.convert(scan as f64) as f32;
                    ms1.rt_values_min.push(rt_min);
                    ms1.mobility_values.push(im);
                    ms1.mz_values.push(mz);
                    ms1.intensity_values.push(intensity);
                    ms1.frame_indices.push(frame.index as u32);
                    ms1.scan_indices.push(scan as u32);
                }
            }
            MSLevel::MS2 => {
                let qs = &frame.quadrupole_settings;
                ms2_pairs.reserve(qs.isolation_mz.len());
                for win in 0..qs.isolation_mz.len() {
                    if win >= qs.isolation_width.len() { break; }
                    let prec_mz = qs.isolation_mz[win] as f32;
                    let width = qs.isolation_width[win] as f32;
                    let low = prec_mz - width * 0.5;
                    let high = prec_mz + width * 0.5;
                    let key = (quantize(low), quantize(high));
                    
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
            }
            _ => {}
        }
        FrameSplit { ms1, ms2: ms2_pairs }
    }).collect();
    
    println!("  Merging frame data...");
    let ms1_size_estimate: usize = splits.par_iter().map(|s| s.ms1.mz_values.len()).sum();
    let mut global_ms1 = TimsTOFData::with_capacity(ms1_size_estimate);
    let mut ms2_hash: HashMap<(u32,u32), TimsTOFData> = HashMap::new();
    
    for split in splits {
        global_ms1.rt_values_min.extend(split.ms1.rt_values_min);
        global_ms1.mobility_values.extend(split.ms1.mobility_values);
        global_ms1.mz_values.extend(split.ms1.mz_values);
        global_ms1.intensity_values.extend(split.ms1.intensity_values);
        global_ms1.frame_indices.extend(split.ms1.frame_indices);
        global_ms1.scan_indices.extend(split.ms1.scan_indices);
        
        for (key, mut td) in split.ms2 {
            ms2_hash.entry(key).or_insert_with(TimsTOFData::new).merge_from(&mut td);
        }
    }
    
    let mut ms2_vec = Vec::with_capacity(ms2_hash.len());
    for ((q_low, q_high), td) in ms2_hash {
        let low = q_low as f32 / 10_000.0;
        let high = q_high as f32 / 10_000.0;
        ms2_vec.push(((low, high), td));
    }
    
    println!("  MS1 data points: {}", global_ms1.mz_values.len());
    println!("  MS2 windows: {}", ms2_vec.len());
    
    Ok(TimsTOFRawData {
        ms1_data: global_ms1,
        ms2_windows: ms2_vec,
    })
}

// ============================================================================
// Index Building Function
// ============================================================================

fn build_indexed_data(raw_data: TimsTOFRawData) -> Result<(IndexedTimsTOFData, Vec<((f32, f32), IndexedTimsTOFData)>), Box<dyn Error>> {
    println!("Building indexed data structures...");
    
    // Build index for MS1 data
    let ms1_indexed = IndexedTimsTOFData::from_timstof_data(raw_data.ms1_data);
    
    // Build index for MS2 windows
    let ms2_indexed_pairs: Vec<((f32, f32), IndexedTimsTOFData)> = raw_data.ms2_windows
        .into_par_iter()
        .map(|((low, high), data)| ((low, high), IndexedTimsTOFData::from_timstof_data(data)))
        .collect();
    
    Ok((ms1_indexed, ms2_indexed_pairs))
}

// ============================================================================
// Main Function
// ============================================================================

fn main() -> Result<(), Box<dyn Error>> {
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    
    // Get data folder path from command line
    let d_folder = if let Some(path) = args.get(1) {
        path.clone()
    } else {
        eprintln!("Usage: {} <path_to_timstof_data.d>", args[0]);
        return Err("No data path provided".into());
    };
    
    let d_path = Path::new(&d_folder);
    if !d_path.exists() {
        return Err(format!("Data folder {:?} not found", d_path).into());
    }
    
    println!("\n========== TimsTOF Data Reader and Indexer ==========");
    println!("Data folder: {}", d_folder);
    
    let total_start = Instant::now();
    
    // Read raw data
    let raw_data_start = Instant::now();
    let raw_data = read_timstof_data(d_path)?;
    println!("Raw data reading time: {:.2} seconds", raw_data_start.elapsed().as_secs_f32());
    
    // Build indexed data
    let index_start = Instant::now();
    let (ms1_indexed, ms2_indexed_pairs) = build_indexed_data(raw_data)?;
    println!("Index building time: {:.2} seconds", index_start.elapsed().as_secs_f32());
    
    println!("\n========== Index Building Complete ==========");
    println!("Total time: {:.2} seconds", total_start.elapsed().as_secs_f32());
    println!("MS1 indexed points: {}", ms1_indexed.mz_values.len());
    println!("MS2 indexed windows: {}", ms2_indexed_pairs.len());
    
    Ok(())
}