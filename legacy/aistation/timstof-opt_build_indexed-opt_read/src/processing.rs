use bitvec::prelude::*;
use crate::utils::{
    TimsTOFRawData, IndexedTimsTOFData, find_scan_for_index, 
    library_records_to_dataframe, merge_library_and_report, get_unique_precursor_ids, 
    process_library_fast, create_rt_im_dicts, build_lib_matrix, build_precursors_matrix_step1, 
    build_precursors_matrix_step2, build_range_matrix_step3, build_precursors_matrix_step3, 
    build_frag_info, get_rt_list, LibCols, quantize, FrameSplit, MergeFrom, PrecursorLibData,
};
use rayon::prelude::*;
use std::{collections::HashMap, error::Error, cmp::Ordering, sync::Arc, time::Instant};
use ndarray::{Array1, Array2, Array3, Array4, s, Axis, concatenate};
use polars::prelude::*;
use std::fs::File;

// 在 processing.rs 中添加

pub fn process_single_precursor(
    precursor_data: &PrecursorLibData,
    ms1_indexed: &IndexedTimsTOFData,
    finder: &FastChunkFinder,
    frag_repeat_num: usize,
    device: &str,
    output_dir: &str,
) -> Result<(), Box<dyn Error>> {
    // let start_time = Instant::now();
    
    // println!("\n========== Processing Precursor: {} ==========", precursor_data.precursor_id);
    // println!("RT: {:.2}, IM: {:.4}", precursor_data.rt, precursor_data.im);
    
    // Step 1: Build tensor representations
    let (ms1_data_tensor, ms2_data_tensor) = build_precursors_matrix_step1(
        &[precursor_data.ms1_data.clone()],
        &[precursor_data.ms2_data.clone()],
        device,
    )?;
    
    let ms2_data_tensor_processed = build_precursors_matrix_step2(ms2_data_tensor);
    
    // Step 2: Build range matrices
    let (ms1_range_list, ms2_range_list) = build_range_matrix_step3(
        &ms1_data_tensor,
        &ms2_data_tensor_processed,
        frag_repeat_num,
        "ppm",
        20.0,
        50.0,
        device,
    )?;
    
    let (re_ms1_data_tensor, re_ms2_data_tensor, ms1_extract_width_range_list, ms2_extract_width_range_list) = 
        build_precursors_matrix_step3(
            &ms1_data_tensor,
            &ms2_data_tensor_processed,
            frag_repeat_num,
            "ppm",
            20.0,
            50.0,
            device,
        )?;
    
    // Step 3: Calculate extraction ranges
    let i = 0; // 因为我们一次只处理一个precursor
    let (ms1_range_min, ms1_range_max) = calculate_mz_range(&ms1_range_list, i);
    let im_tolerance = 0.05f32;
    let im_min = precursor_data.im - im_tolerance;
    let im_max = precursor_data.im + im_tolerance;
    
    let precursor_mz = precursor_data.precursor_info[1]; // precursor_info的第二个元素是precursor_mz
    
    // Step 4: Extract MS1 data
    let mut precursor_result_filtered = ms1_indexed.slice_by_mz_im_range(
        ms1_range_min, ms1_range_max, im_min, im_max
    );
    precursor_result_filtered.mz_values.iter_mut()
        .for_each(|mz| *mz = (*mz * 1000.0).ceil());
    
    // Step 5: Extract MS2 data
    let mut frag_result_filtered = extract_ms2_data(
        finder,
        precursor_mz,
        &ms2_range_list,
        i,
        im_min,
        im_max,
    )?;
    
    // Step 6: Build mask matrices
    let (ms1_frag_moz_matrix, ms2_frag_moz_matrix) = build_mask_matrices(
        &precursor_result_filtered,
        &frag_result_filtered,
        &ms1_extract_width_range_list,
        &ms2_extract_width_range_list,
        i,
    )?;
    
    // Step 7: Extract aligned RT values
    let all_rt = extract_aligned_rt_values(
        &precursor_result_filtered,
        &frag_result_filtered,
        precursor_data.rt,
    );
    
    // Step 8: Build intensity matrices
    let ms1_extract_slice = ms1_extract_width_range_list.slice(s![i, .., ..]).to_owned();
    let ms2_extract_slice = ms2_extract_width_range_list.slice(s![i, .., ..]).to_owned();
    
    let ms1_frag_rt_matrix = build_intensity_matrix_optimized(
        &precursor_result_filtered,
        &ms1_extract_slice,
        &ms1_frag_moz_matrix,
        &all_rt,
    )?;
    
    let ms2_frag_rt_matrix = build_intensity_matrix_optimized(
        &frag_result_filtered,
        &ms2_extract_slice,
        &ms2_frag_moz_matrix,
        &all_rt,
    )?;
    
    // Step 9: Reshape and combine matrices
    let rsm_matrix = reshape_and_combine_matrices(
        ms1_frag_rt_matrix,
        ms2_frag_rt_matrix,
        frag_repeat_num,
    )?;
    
    // Step 10: Build fragment info
    let frag_info = build_frag_info(
        &ms1_data_tensor,
        &ms2_data_tensor_processed,
        frag_repeat_num,
        device,
    );
    
    // Step 11: Create final dataframe
    let final_df = create_final_dataframe(
        &rsm_matrix,
        &frag_info,
        &all_rt,
        i,
    )?;
    
    // // Step 12: Save results with precursor info in filename
    let output_filename = format!(
        "{}/{}_RT{:.2}_IM{:.4}_final_dataframe.csv",
        output_dir,
        precursor_data.precursor_id,
        precursor_data.rt,
        precursor_data.im
    );
    
    let mut file = File::create(&output_filename)?;
    CsvWriter::new(&mut file)
        .include_header(true)
        .finish(&mut final_df.clone())?;
    
    // println!("Processing time: {:.3} seconds", start_time.elapsed().as_secs_f32());
    // println!("Output saved to: {}", output_filename);
    
    Ok(())
}

pub struct FastChunkFinder {
    low_bounds: Vec<f32>,
    high_bounds: Vec<f32>,
    chunks: Vec<IndexedTimsTOFData>,
}

impl FastChunkFinder {
    pub fn new(mut pairs: Vec<((f32, f32), IndexedTimsTOFData)>) -> Result<Self, Box<dyn Error>> {
        if pairs.is_empty() { return Err("no MS2 windows collected".into()); }
        pairs.sort_by(|a, b| a.0 .0.partial_cmp(&b.0 .0).unwrap());
        
        let n = pairs.len();
        let mut low = Vec::with_capacity(n);
        let mut high = Vec::with_capacity(n);
        for ((l, h), _) in &pairs {
            low.push(*l);
            high.push(*h);
        }
        
        let chunks: Vec<IndexedTimsTOFData> = pairs.into_iter().map(|(_, data)| data).collect();
        Ok(Self { low_bounds: low, high_bounds: high, chunks })
    }
    
    #[inline]
    pub fn find(&self, mz: f32) -> Option<&IndexedTimsTOFData> {
        match self.low_bounds.binary_search_by(|probe| probe.partial_cmp(&mz).unwrap()) {
            Ok(idx) => Some(&self.chunks[idx]),
            Err(0) => None,
            Err(pos) => {
                let idx = pos - 1;
                if mz <= self.high_bounds[idx] { Some(&self.chunks[idx]) } else { None }
            }
        }
    }
}

pub fn build_intensity_matrix_optimized(
    // 提前建立时间→索引的映射，避免重复计算。
    // 这就是整个优化的核心原理：从"查找所有可能"变成"只处理存在的数据"。
    data: &crate::utils::TimsTOFData,
    extract_width_range: &Array2<f32>,
    frag_moz_matrix: &Array2<f32>,
    all_rt: &[f32],
) -> Result<Array2<f32>, Box<dyn Error>> {
    use ahash::AHashMap as HashMap;
    use ndarray::{Array2, Axis};

    let n_rt = all_rt.len();
    let n_frags = extract_width_range.shape()[0];

    // 1. 构建 RT 索引映射
    let rt_keys: Vec<i32> = all_rt.iter().map(|&rt| (rt * 1e6) as i32).collect();
    let mut rt2idx: HashMap<i32, usize> = HashMap::with_capacity(rt_keys.len());
    for (idx, &key) in rt_keys.iter().enumerate() {
        rt2idx.insert(key, idx);
    }

    // 2. 构建稀疏数据表
    let mut mz_table: HashMap<i32, Vec<(usize, f32)>> =
        HashMap::with_capacity(data.mz_values.len() / 4);

    for ((&mz_f, &rt_f), &inten_f) in data.mz_values
                                        .iter()
                                        .zip(&data.rt_values_min)
                                        .zip(&data.intensity_values)
    {
        let mz_key = mz_f as i32;
        let rt_key = (rt_f * 1e6) as i32;

        if let Some(&rt_idx) = rt2idx.get(&rt_key) {
            mz_table.entry(mz_key)
                    .or_insert_with(Vec::new)
                    .push((rt_idx, inten_f as f32));
        }
    }

    // 3. 高效矩阵填充
    let mut frag_rt_matrix = Array2::<f32>::zeros((n_frags, n_rt));

    for (frag_idx, mut row) in frag_rt_matrix.axis_iter_mut(Axis(0)).enumerate() {
        for mz_idx in 0..extract_width_range.shape()[1] {
            let mz_key = extract_width_range[[frag_idx, mz_idx]] as i32;
            let mask = frag_moz_matrix[[frag_idx, mz_idx]];
            if mask == 0.0 { continue; }

            if let Some(entries) = mz_table.get(&mz_key) {
                for &(rt_idx, inten) in entries {
                    row[rt_idx] += mask * inten;
                }
            }
        }
    }

    Ok(frag_rt_matrix)
}

// Helper function implementations

pub fn prepare_precursor_features(
    precursors_list: &[Vec<String>],
    precursor_info_list: &[Vec<f32>],
    assay_rt_kept_dict: &std::collections::HashMap<String, f32>,
    assay_im_kept_dict: &std::collections::HashMap<String, f32>,
) -> Result<Array2<f32>, Box<dyn Error>> {
    let n_precursors = precursors_list.len();
    let n_cols = 8; // 5 info columns + im + rt + delta_rt
    
    let mut precursor_feat = Array2::<f32>::zeros((n_precursors, n_cols));
    
    for i in 0..n_precursors {
        // Copy first 5 columns from precursor_info
        let info_len = precursor_info_list[i].len().min(5);
        for j in 0..info_len {
            precursor_feat[[i, j]] = precursor_info_list[i][j];
        }
        
        // Add assay IM and RT
        precursor_feat[[i, 5]] = assay_im_kept_dict
            .get(&precursors_list[i][0])
            .copied()
            .unwrap_or(0.0);
        
        precursor_feat[[i, 6]] = assay_rt_kept_dict
            .get(&precursors_list[i][0])
            .copied()
            .unwrap_or(0.0);
        
        // Delta RT is 0 for all
        precursor_feat[[i, 7]] = 0.0;
    }
    
    Ok(precursor_feat)
}

pub fn calculate_mz_range(ms1_range_list: &Array3<f32>, i: usize) -> (f32, f32) {
    let ms1_range_slice = ms1_range_list.slice(s![i, .., ..]);
    
    let ms1_range_min_val = ms1_range_slice
        .iter()
        .filter(|&&v| v > 0.0)
        .fold(f32::INFINITY, |a, &b| a.min(b));
    
    let ms1_range_max_val = ms1_range_slice
        .iter()
        .fold(f32::NEG_INFINITY, |a, &b| a.max(b));
    
    let ms1_range_min = (ms1_range_min_val - 1.0) / 1000.0;
    let ms1_range_max = (ms1_range_max_val + 1.0) / 1000.0;
    
    (ms1_range_min, ms1_range_max)
}

pub fn extract_ms2_data(
    finder: &FastChunkFinder,
    precursor_mz: f32,
    ms2_range_list: &Array3<f32>,
    i: usize,
    im_min: f32,
    im_max: f32,
) -> Result<crate::utils::TimsTOFData, Box<dyn Error>> {
    let mut result = if let Some(ms2_indexed) = finder.find(precursor_mz) {
        // Process all 66 MS2 ranges in parallel
        let frag_results: Vec<crate::utils::TimsTOFData> = (0..66)
            .into_iter()
            .map(|j| {
                let ms2_range_min_val = ms2_range_list[[i, j, 0]];
                let ms2_range_max_val = ms2_range_list[[i, j, 1]];
                
                let ms2_range_min = (ms2_range_min_val - 1.0) / 1000.0;
                let ms2_range_max = (ms2_range_max_val + 1.0) / 1000.0;
                
                if ms2_range_min <= 0.0 || ms2_range_max <= 0.0 || ms2_range_min >= ms2_range_max {
                    crate::utils::TimsTOFData::new()
                } else {
                    ms2_indexed.slice_by_mz_im_range(
                        ms2_range_min, ms2_range_max, im_min, im_max
                    )
                }
            })
            .collect();
        
        crate::utils::TimsTOFData::merge(frag_results)
    } else {
        println!("  Warning: No MS2 data found for precursor m/z {:.4}", precursor_mz);
        crate::utils::TimsTOFData::new()
    };
    
    // Convert m/z values to integers
    result.mz_values.iter_mut()
        .for_each(|mz| *mz = (*mz * 1000.0).ceil());
    
    Ok(result)
}

pub fn build_mask_matrices(
    precursor_result_filtered: &crate::utils::TimsTOFData,
    frag_result_filtered: &crate::utils::TimsTOFData,
    ms1_extract_width_range_list: &Array3<f32>,
    ms2_extract_width_range_list: &Array3<f32>,
    i: usize,
) -> Result<(Array2<f32>, Array2<f32>), Box<dyn Error>> {
    use std::collections::HashSet;
    
    // Create hash sets for fast lookup
    let search_ms1_set: HashSet<i32> = precursor_result_filtered.mz_values
        .iter()
        .map(|&mz| mz as i32)
        .collect();
    
    let search_ms2_set: HashSet<i32> = frag_result_filtered.mz_values
        .iter()
        .map(|&mz| mz as i32)
        .collect();
    
    // Extract slices
    let ms1_extract_slice = ms1_extract_width_range_list.slice(s![i, .., ..]);
    let ms2_extract_slice = ms2_extract_width_range_list.slice(s![i, .., ..]);
    
    // Build MS1 mask matrix
    let (n_frags_ms1, n_mz_ms1) = (ms1_extract_slice.shape()[0], ms1_extract_slice.shape()[1]);
    let mut ms1_frag_moz_matrix = Array2::<f32>::zeros((n_frags_ms1, n_mz_ms1));
    
    for j in 0..n_frags_ms1 {
        for k in 0..n_mz_ms1 {
            let val = ms1_extract_slice[[j, k]] as i32;
            if val > 0 && search_ms1_set.contains(&val) {
                ms1_frag_moz_matrix[[j, k]] = 1.0;
            }
        }
    }
    
    // Build MS2 mask matrix
    let (n_frags_ms2, n_mz_ms2) = (ms2_extract_slice.shape()[0], ms2_extract_slice.shape()[1]);
    let mut ms2_frag_moz_matrix = Array2::<f32>::zeros((n_frags_ms2, n_mz_ms2));
    
    for j in 0..n_frags_ms2 {
        for k in 0..n_mz_ms2 {
            let val = ms2_extract_slice[[j, k]] as i32;
            if val > 0 && search_ms2_set.contains(&val) {
                ms2_frag_moz_matrix[[j, k]] = 1.0;
            }
        }
    }
    
    Ok((ms1_frag_moz_matrix, ms2_frag_moz_matrix))
}

pub fn extract_aligned_rt_values(
    precursor_result_filtered: &crate::utils::TimsTOFData,
    frag_result_filtered: &crate::utils::TimsTOFData,
    target_rt: f32,
) -> Vec<f32> {
    use std::collections::HashSet;
    
    let mut all_rt_set = HashSet::new();
    
    // Collect all unique RT values
    for &rt_val in &precursor_result_filtered.rt_values_min {
        all_rt_set.insert((rt_val * 1e6) as i32);
    }
    
    for &rt_val in &frag_result_filtered.rt_values_min {
        all_rt_set.insert((rt_val * 1e6) as i32);
    }
    
    // Convert to sorted vector
    let mut all_rt_vec: Vec<f32> = all_rt_set
        .iter()
        .map(|&rt_int| rt_int as f32 / 1e6)
        .collect();
    
    all_rt_vec.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    
    // Get RT list with target RT in the center
    get_rt_list(all_rt_vec, target_rt)
}

pub fn reshape_and_combine_matrices(
    ms1_frag_rt_matrix: Array2<f32>,
    ms2_frag_rt_matrix: Array2<f32>,
    frag_repeat_num: usize,
) -> Result<Array4<f32>, Box<dyn Error>> {
    // Reshape MS1 matrix
    let (ms1_rows, ms1_cols) = ms1_frag_rt_matrix.dim();
    let ms1_reshaped = ms1_frag_rt_matrix.into_shape((
        frag_repeat_num,
        ms1_rows / frag_repeat_num,
        ms1_cols
    ))?;
    
    // Reshape MS2 matrix
    let (ms2_rows, ms2_cols) = ms2_frag_rt_matrix.dim();
    let ms2_reshaped = ms2_frag_rt_matrix.into_shape((
        frag_repeat_num,
        ms2_rows / frag_repeat_num,
        ms2_cols
    ))?;
    
    // Combine matrices
    let ms1_frags = ms1_reshaped.shape()[1];
    let ms2_frags = ms2_reshaped.shape()[1];
    let total_frags = ms1_frags + ms2_frags;
    let n_rt = ms1_reshaped.shape()[2];
    
    let mut full_frag_rt_matrix = Array3::<f32>::zeros((frag_repeat_num, total_frags, n_rt));
    
    // Copy MS1 data
    full_frag_rt_matrix.slice_mut(s![.., ..ms1_frags, ..])
        .assign(&ms1_reshaped);
    
    // Copy MS2 data
    full_frag_rt_matrix.slice_mut(s![.., ms1_frags.., ..])
        .assign(&ms2_reshaped);
    
    // Add batch dimension
    Ok(full_frag_rt_matrix.insert_axis(Axis(0)))
}

pub fn create_final_dataframe(
    rsm_matrix: &Array4<f32>,
    frag_info: &Array3<f32>,
    all_rt: &[f32],
    i: usize,
) -> Result<DataFrame, Box<dyn Error>> {
    // Aggregate across repeat dimension
    let aggregated_x_sum = rsm_matrix.sum_axis(Axis(1));
    
    // Extract data for this precursor
    let precursor_data = aggregated_x_sum.slice(s![0, .., ..]);
    let precursor_frag_info = frag_info.slice(s![i, .., ..]);
    
    let total_frags = precursor_data.shape()[0];
    let mut columns = Vec::new();
    
    // Add RT columns
    for (rt_idx, &rt_val) in all_rt.iter().enumerate() {
        let col_data: Vec<f32> = (0..total_frags)
            .map(|frag_idx| precursor_data[[frag_idx, rt_idx]])
            .collect();
        columns.push(Series::new(&format!("{:.6}", rt_val), col_data));
    }
    
    // Add fragment info columns
    let info_names = ["ProductMz", "LibraryIntensity", "frag_type", "FragmentType"];
    for col_idx in 0..4.min(precursor_frag_info.shape()[1]) {
        let col_data: Vec<f32> = (0..total_frags)
            .map(|row_idx| precursor_frag_info[[row_idx, col_idx]])
            .collect();
        columns.push(Series::new(info_names[col_idx], col_data));
    }
    
    Ok(DataFrame::new(columns)?)
}