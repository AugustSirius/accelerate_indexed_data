mod utils;
mod cache;
mod processing;

use std::sync::{Arc, Mutex};
use std::io::BufWriter;

use cache::CacheManager;
use utils::{
    read_timstof_data, build_indexed_data, read_parquet_with_polars,IndexedTimsTOFData,
    library_records_to_dataframe, merge_library_and_report, get_unique_precursor_ids, 
    process_library_fast, create_rt_im_dicts, build_lib_matrix, build_precursors_matrix_step1, 
    build_precursors_matrix_step2, build_range_matrix_step3, build_precursors_matrix_step3, 
    build_frag_info, LibCols, PrecursorLibData, prepare_precursor_lib_data,
    extract_unique_rt_im_values, save_unique_values_to_files, UniqueValues
};
use processing::{
    FastChunkFinder, build_rt_intensity_matrix_optimized, prepare_precursor_features,
    calculate_mz_range, extract_ms2_data, build_mask_matrices, extract_aligned_rt_values,
    reshape_and_combine_matrices, process_single_precursor_rsm
};

use rayon::prelude::*;
use std::{error::Error, path::Path, time::Instant, env, fs::File};
use ndarray::{Array1, Array2, Array3, Array4, s, Axis};
use polars::prelude::*;
use ndarray_npy::{NpzWriter, write_npy};

// Speed test parameters
const SPEED_TEST_MODE: bool = true;  // Set to false to process ALL precursors
const SPEED_TEST_PRECURSOR_COUNT: usize = 10000;  // Number of precursors for speed test

// Performance monitoring struct
#[derive(Debug, Clone)]
pub struct StepTimings {
    pub matrix_building: f64,
    pub ms1_extraction: f64,
    pub ms2_extraction: f64,
    pub mask_building: f64,
    pub rt_extraction: f64,
    pub intensity_matrix: f64,
    pub reshape_combine: f64,
    pub total: f64,
}

impl StepTimings {
    fn new() -> Self {
        Self {
            matrix_building: 0.0,
            ms1_extraction: 0.0,
            ms2_extraction: 0.0,
            mask_building: 0.0,
            rt_extraction: 0.0,
            intensity_matrix: 0.0,
            reshape_combine: 0.0,
            total: 0.0,
        }
    }
}

// New struct to hold RSM results
#[derive(Debug)]
pub struct RSMPrecursorResults {
    pub index: usize,
    pub precursor_id: String,
    pub rsm_matrix: Array4<f32>,  // Shape: [1, 5, 72, 396]
    pub all_rt: Vec<f32>,          // 396 RT values
}

fn main() -> Result<(), Box<dyn Error>> {
    // Fixed parameters
    let batch_size = 1000;
    let parallel_threads = 64;  // Set to 64 as requested
    let output_dir = if SPEED_TEST_MODE { "output_diann_speedtest" } else { "output_diann" };
    
    let d_folder = "/wangshuaiyao/dia-bert-timstof/test_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d";
    let report_file_path = "/wangshuaiyao/dia-bert-timstof/test_data/report.parquet";
    let lib_file_path = "/wangshuaiyao/dia-bert-timstof/lib/TPHPlib_frag1025_swissprot_final_all_from_Yueliang_with_decoy.tsv";
    
    println!("\n========== PERFORMANCE MONITORING ENABLED ==========");
    if SPEED_TEST_MODE {
        println!("🚀 SPEED TEST MODE ENABLED - Processing first {} precursors only", SPEED_TEST_PRECURSOR_COUNT);
    } else {
        println!("📊 FULL PROCESSING MODE - Processing ALL precursors");
    }
    println!("Parallel threads: {}", parallel_threads);
    
    rayon::ThreadPoolBuilder::new()
        .num_threads(parallel_threads)
        .build_global()
        .unwrap();
    
    println!("Initialized parallel processing with {} threads", parallel_threads);
    println!("Batch size: {}", batch_size);
    println!("Output directory: {}", output_dir);
    
    let d_path = Path::new(&d_folder);
    if !d_path.exists() {
        return Err(format!("folder {:?} not found", d_path).into());
    }
    
    // Create output directory
    std::fs::create_dir_all(&output_dir)?;
    
    // ================================ DATA LOADING AND INDEXING ================================
    let cache_manager = CacheManager::new();
    
    println!("\n========== DATA PREPARATION PHASE ==========");
    let total_start = Instant::now();
        
    let (ms1_indexed, ms2_indexed_pairs) = if cache_manager.is_cache_valid(d_path) {
        println!("Found valid cache, loading indexed data directly...");
        let cache_load_start = Instant::now();
        let (ms1_indexed, ms2_indexed_pairs) = cache_manager.load_indexed_data(d_path)?;
        println!("Cache loading time: {:.5} seconds", cache_load_start.elapsed().as_secs_f32());
        (ms1_indexed, ms2_indexed_pairs)
    } else {
        println!("Cache invalid or non-existent, reading TimsTOF data...");
        
        let raw_data_start = Instant::now();
        let raw_data = read_timstof_data(d_path)?;
        println!("Raw data reading time: {:.5} seconds", raw_data_start.elapsed().as_secs_f32());
        println!("  - MS1 data points: {}", raw_data.ms1_data.mz_values.len());
        println!("  - MS2 windows: {}", raw_data.ms2_windows.len());
        
        println!("\nBuilding indexed data structures...");
        let index_start = Instant::now();
        let (ms1_indexed, ms2_indexed_pairs) = build_indexed_data(raw_data)?;
        println!("Index building time: {:.5} seconds", index_start.elapsed().as_secs_f32());
        
        let cache_save_start = Instant::now();
        cache_manager.save_indexed_data(d_path, &ms1_indexed, &ms2_indexed_pairs)?;
        println!("Cache saving time: {:.5} seconds", cache_save_start.elapsed().as_secs_f32());
        
        (ms1_indexed, ms2_indexed_pairs)
    };
    
    println!("Total data preparation time: {:.5} seconds", total_start.elapsed().as_secs_f32());
    
    let finder = FastChunkFinder::new(ms2_indexed_pairs)?;
    
    // ================================ LIBRARY AND REPORT LOADING ================================
    println!("\n========== LIBRARY AND REPORT PROCESSING ==========");
    let lib_processing_start = Instant::now();

    let library_records = process_library_fast(lib_file_path)?;
    let library_df = library_records_to_dataframe(library_records.clone())?;

    let report_df = read_parquet_with_polars(report_file_path)?;
    let diann_result = merge_library_and_report(library_df, report_df)?;
    
    let diann_precursor_id_all = get_unique_precursor_ids(&diann_result)?;
    println!("diann_precursor_id_all: {:?}", diann_precursor_id_all.head(Some(5)));
    let (assay_rt_kept_dict, assay_im_kept_dict) = create_rt_im_dicts(&diann_precursor_id_all)?;
    
    println!("Library and report processing time: {:.5} seconds", lib_processing_start.elapsed().as_secs_f32());
    
    let device = "cpu";
    let frag_repeat_num = 5;
    
    // ================================ BATCH PRECURSOR PROCESSING ================================
    println!("\n========== BATCH PRECURSOR PROCESSING ==========");
    
    println!("\n[Step 1] Preparing library data for precursors");
    let prep_start = Instant::now();
    
    let unique_precursor_ids: Vec<String> = diann_precursor_id_all
        .column("transition_group_id")?
        .str()?
        .into_iter()
        .filter_map(|opt| opt.map(|s| s.to_string()))
        .collect();

    let total_unique_precursors = unique_precursor_ids.len();
    println!("\n========== LIBRARY STATISTICS ==========");
    println!("Total unique precursor IDs in library: {}", total_unique_precursors);
    
    // Determine how many precursors to process
    let precursors_to_process = if SPEED_TEST_MODE {
        SPEED_TEST_PRECURSOR_COUNT.min(total_unique_precursors)
    } else {
        total_unique_precursors
    };
    
    if SPEED_TEST_MODE {
        println!("🚀 Speed test mode: Processing only first {} precursors", precursors_to_process);
    }
    
    let lib_cols = LibCols::default();
    
    // Process precursors based on mode
    let precursor_lib_data_list = prepare_precursor_lib_data(
        &library_records,
        &unique_precursor_ids,
        &assay_rt_kept_dict,
        &assay_im_kept_dict,
        &lib_cols,
        precursors_to_process,  // Use determined count
    )?;
    
    println!("  - Prepared data for {} precursors", precursor_lib_data_list.len());
    println!("  - Preparation time: {:.5} seconds", prep_start.elapsed().as_secs_f32());
    
    drop(library_records);
    println!("  - Released library_records from memory");
    
    // Process in batches
    let total_batches = (precursor_lib_data_list.len() + batch_size - 1) / batch_size;
    println!("\n[Step 2] Processing {} precursors in {} batches", 
             precursor_lib_data_list.len(), total_batches);
    
    // Global performance tracking
    let mut global_step_timings = StepTimings::new();
    let mut global_precursor_count = 0;
    let overall_processing_start = Instant::now();
    
    for batch_idx in 0..total_batches {
        let batch_start_idx = batch_idx * batch_size;
        let batch_end_idx = ((batch_idx + 1) * batch_size).min(precursor_lib_data_list.len());
        let batch_precursors = &precursor_lib_data_list[batch_start_idx..batch_end_idx];
        
        println!("\n========== Processing Batch {}/{} ==========", batch_idx + 1, total_batches);
        println!("Precursors {} to {} (total: {})", 
                 batch_start_idx + 1, batch_end_idx, batch_precursors.len());
        
        let batch_start = Instant::now();
        
        use std::sync::atomic::{AtomicUsize, Ordering};
        let processed_count = Arc::new(AtomicUsize::new(0));
        let batch_count = batch_precursors.len();
        
        let results_mutex = Arc::new(Mutex::new(Vec::new()));
        let step_timings_mutex = Arc::new(Mutex::new(StepTimings::new()));
        
        // Process batch in parallel with detailed timing
        batch_precursors
            .par_iter()
            .enumerate()
            .for_each(|(batch_internal_idx, precursor_data)| {
                let global_index = batch_start_idx + batch_internal_idx;
                let thread_id = rayon::current_thread_index().unwrap_or(999);
                
                // Process with detailed timing
                let result = process_single_precursor_with_timing(
                    precursor_data,
                    &ms1_indexed,
                    &finder,
                    frag_repeat_num,
                    device,
                    thread_id,
                    &step_timings_mutex,
                );
                
                let current = processed_count.fetch_add(1, Ordering::SeqCst) + 1;
                
                // Print progress every 100 precursors
                if current % 100 == 0 {
                    println!("[Batch {} - Progress: {}/{}] Processing rate: {:.2} precursors/sec", 
                             batch_idx + 1, current, batch_count,
                             current as f32 / batch_start.elapsed().as_secs_f32());
                }
                
                match result {
                    Ok((precursor_id, rsm_matrix, all_rt)) => {
                        let rsm_result = RSMPrecursorResults {
                            index: batch_internal_idx,
                            precursor_id: precursor_id.clone(),
                            rsm_matrix,
                            all_rt,
                        };
                        
                        let mut results = results_mutex.lock().unwrap();
                        results.push(rsm_result);
                    },
                    Err(e) => {
                        eprintln!("[Batch {} - {}/{}] ✗ Error processing {} (global index: {}): {}", 
                                  batch_idx + 1, current, batch_count, 
                                  precursor_data.precursor_id, global_index, e);
                    }
                }
            });
        
        let batch_elapsed = batch_start.elapsed();
        
        // Print batch timing summary
        let batch_timings = step_timings_mutex.lock().unwrap().clone();
        println!("\n========== BATCH {} TIMING BREAKDOWN ==========", batch_idx + 1);
        println!("Total batch processing time: {:.5} seconds", batch_elapsed.as_secs_f32());
        println!("Average time per precursor: {:.5} seconds", batch_elapsed.as_secs_f32() / batch_count as f32);
        println!("\nAverage step timings (ms per precursor):");
        println!("  Matrix building:    {:.3} ms", batch_timings.matrix_building / batch_count as f64 * 1000.0);
        println!("  MS1 extraction:     {:.3} ms", batch_timings.ms1_extraction / batch_count as f64 * 1000.0);
        println!("  MS2 extraction:     {:.3} ms", batch_timings.ms2_extraction / batch_count as f64 * 1000.0);
        println!("  Mask building:      {:.3} ms", batch_timings.mask_building / batch_count as f64 * 1000.0);
        println!("  RT extraction:      {:.3} ms", batch_timings.rt_extraction / batch_count as f64 * 1000.0);
        println!("  Intensity matrix:   {:.3} ms", batch_timings.intensity_matrix / batch_count as f64 * 1000.0);
        println!("  Reshape & combine:  {:.3} ms", batch_timings.reshape_combine / batch_count as f64 * 1000.0);
        
        // Update global timings
        global_step_timings.matrix_building += batch_timings.matrix_building;
        global_step_timings.ms1_extraction += batch_timings.ms1_extraction;
        global_step_timings.ms2_extraction += batch_timings.ms2_extraction;
        global_step_timings.mask_building += batch_timings.mask_building;
        global_step_timings.rt_extraction += batch_timings.rt_extraction;
        global_step_timings.intensity_matrix += batch_timings.intensity_matrix;
        global_step_timings.reshape_combine += batch_timings.reshape_combine;
        global_precursor_count += batch_count;
        
        println!("\n========== SAVING BATCH {} RESULTS ==========", batch_idx + 1);
        let save_start = Instant::now();
        
        let mut results = Arc::try_unwrap(results_mutex).unwrap().into_inner().unwrap();
        
        // Sort results by original index to restore order
        results.sort_by_key(|r| r.index);
        
        // Save batch results
        save_batch_results_as_npy(&results, batch_precursors, batch_idx, &output_dir)?;
        
        println!("Batch {} save time: {:.5} seconds", batch_idx + 1, save_start.elapsed().as_secs_f32());
        
        println!("\n========== BATCH {} SUMMARY ==========", batch_idx + 1);
        println!("Processed: {} precursors", results.len());
        println!("Failed: {} precursors", batch_count - results.len());
        println!("Batch processing time: {:.5} seconds", batch_elapsed.as_secs_f32());
        println!("Average time per precursor: {:.5} seconds", 
                 batch_elapsed.as_secs_f32() / batch_count as f32);
    }
    
    let overall_processing_time = overall_processing_start.elapsed();
    
    // Print overall performance summary
    println!("\n========== OVERALL PERFORMANCE SUMMARY ==========");
    if SPEED_TEST_MODE {
        println!("🚀 SPEED TEST MODE RESULTS");
        println!("Processed: {} precursors (out of {} total in library)", 
                 precursor_lib_data_list.len(), total_unique_precursors);
    } else {
        println!("📊 FULL PROCESSING MODE RESULTS");
        println!("Total unique precursor IDs in library: {}", total_unique_precursors);
        println!("Total processed: {} precursors", precursor_lib_data_list.len());
    }
    
    println!("Processing mode: Parallel ({} threads)", parallel_threads);
    println!("Batch size: {}", batch_size);
    println!("Total batches: {}", total_batches);
    println!("Total processing time: {:.2} seconds", overall_processing_time.as_secs_f32());
    println!("Overall throughput: {:.2} precursors/second", 
             precursor_lib_data_list.len() as f32 / overall_processing_time.as_secs_f32());
    
    println!("\nGlobal average step timings (ms per precursor):");
    println!("  Matrix building:    {:.3} ms ({:.1}%)", 
             global_step_timings.matrix_building / global_precursor_count as f64 * 1000.0,
             global_step_timings.matrix_building / global_step_timings.total * 100.0);
    println!("  MS1 extraction:     {:.3} ms ({:.1}%)", 
             global_step_timings.ms1_extraction / global_precursor_count as f64 * 1000.0,
             global_step_timings.ms1_extraction / global_step_timings.total * 100.0);
    println!("  MS2 extraction:     {:.3} ms ({:.1}%)", 
             global_step_timings.ms2_extraction / global_precursor_count as f64 * 1000.0,
             global_step_timings.ms2_extraction / global_step_timings.total * 100.0);
    println!("  Mask building:      {:.3} ms ({:.1}%)", 
             global_step_timings.mask_building / global_precursor_count as f64 * 1000.0,
             global_step_timings.mask_building / global_step_timings.total * 100.0);
    println!("  RT extraction:      {:.3} ms ({:.1}%)", 
             global_step_timings.rt_extraction / global_precursor_count as f64 * 1000.0,
             global_step_timings.rt_extraction / global_step_timings.total * 100.0);
    println!("  Intensity matrix:   {:.3} ms ({:.1}%)", 
             global_step_timings.intensity_matrix / global_precursor_count as f64 * 1000.0,
             global_step_timings.intensity_matrix / global_step_timings.total * 100.0);
    println!("  Reshape & combine:  {:.3} ms ({:.1}%)", 
             global_step_timings.reshape_combine / global_precursor_count as f64 * 1000.0,
             global_step_timings.reshape_combine / global_step_timings.total * 100.0);
    
    println!("\nOutput directory: {}", output_dir);
    
    Ok(())
}

// New function with detailed timing
fn process_single_precursor_with_timing(
    precursor_data: &PrecursorLibData,
    ms1_indexed: &IndexedTimsTOFData,
    finder: &FastChunkFinder,
    frag_repeat_num: usize,
    device: &str,
    thread_id: usize,
    step_timings_mutex: &Arc<Mutex<StepTimings>>,
) -> Result<(String, Array4<f32>, Vec<f32>), Box<dyn Error>> {
    let total_start = Instant::now();
    let mut local_timings = StepTimings::new();
    
    // Step 1: Build tensor representations
    let step_start = Instant::now();
    let (ms1_data_tensor, ms2_data_tensor) = build_precursors_matrix_step1(
        &[precursor_data.ms1_data.clone()],
        &[precursor_data.ms2_data.clone()],
        device,
    )?;
    
    let ms2_data_tensor_processed = build_precursors_matrix_step2(ms2_data_tensor);
    
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
    local_timings.matrix_building = step_start.elapsed().as_secs_f64();
    
    // Step 2: Extract MS1 data
    let step_start = Instant::now();
    let i = 0;
    let (ms1_range_min, ms1_range_max) = calculate_mz_range(&ms1_range_list, i);
    let im_tolerance = 0.05f32;
    let im_min = precursor_data.im - im_tolerance;
    let im_max = precursor_data.im + im_tolerance;
    
    let mut precursor_result_filtered = ms1_indexed.slice_by_mz_im_range(
        ms1_range_min, ms1_range_max, im_min, im_max
    );
    precursor_result_filtered.mz_values.iter_mut()
        .for_each(|mz| *mz = (*mz * 1000.0).ceil());
    local_timings.ms1_extraction = step_start.elapsed().as_secs_f64();
    
    // Step 3: Extract MS2 data
    let step_start = Instant::now();
    let precursor_mz = precursor_data.precursor_info[1];
    let mut frag_result_filtered = extract_ms2_data(
        finder,
        precursor_mz,
        &ms2_range_list,
        i,
        im_min,
        im_max,
    )?;
    local_timings.ms2_extraction = step_start.elapsed().as_secs_f64();
    
    // Step 4: Build mask matrices
    let step_start = Instant::now();
    let (ms1_frag_moz_matrix, ms2_frag_moz_matrix) = build_mask_matrices(
        &precursor_result_filtered,
        &frag_result_filtered,
        &ms1_extract_width_range_list,
        &ms2_extract_width_range_list,
        i,
    )?;
    local_timings.mask_building = step_start.elapsed().as_secs_f64();
    
    // Step 5: Extract aligned RT values
    let step_start = Instant::now();
    let all_rt = extract_aligned_rt_values(
        &precursor_result_filtered,
        &frag_result_filtered,
        precursor_data.rt,
    );
    local_timings.rt_extraction = step_start.elapsed().as_secs_f64();
    
    // Step 6: Build intensity matrices
    let step_start = Instant::now();
    let ms1_extract_slice = ms1_extract_width_range_list.slice(s![i, .., ..]).to_owned();
    let ms2_extract_slice = ms2_extract_width_range_list.slice(s![i, .., ..]).to_owned();
    
    let ms1_frag_rt_matrix = build_rt_intensity_matrix_optimized(
        &precursor_result_filtered,
        &ms1_extract_slice,
        &ms1_frag_moz_matrix,
        &all_rt,
    )?;
    
    let ms2_frag_rt_matrix = build_rt_intensity_matrix_optimized(
        &frag_result_filtered,
        &ms2_extract_slice,
        &ms2_frag_moz_matrix,
        &all_rt,
    )?;
    local_timings.intensity_matrix = step_start.elapsed().as_secs_f64();
    
    // Step 7: Reshape and combine matrices
    let step_start = Instant::now();
    let rsm_matrix = reshape_and_combine_matrices(
        ms1_frag_rt_matrix,
        ms2_frag_rt_matrix,
        frag_repeat_num,
    )?;
    local_timings.reshape_combine = step_start.elapsed().as_secs_f64();
    
    local_timings.total = total_start.elapsed().as_secs_f64();
    
    // Update shared timings
    {
        let mut shared_timings = step_timings_mutex.lock().unwrap();
        shared_timings.matrix_building += local_timings.matrix_building;
        shared_timings.ms1_extraction += local_timings.ms1_extraction;
        shared_timings.ms2_extraction += local_timings.ms2_extraction;
        shared_timings.mask_building += local_timings.mask_building;
        shared_timings.rt_extraction += local_timings.rt_extraction;
        shared_timings.intensity_matrix += local_timings.intensity_matrix;
        shared_timings.reshape_combine += local_timings.reshape_combine;
        shared_timings.total += local_timings.total;
    }
    
    // Log slow precursors (taking more than 50ms)
    if local_timings.total > 0.05 {
        println!("[Thread {}] Slow precursor {}: {:.3}s total (matrix:{:.3}, ms1:{:.3}, ms2:{:.3}, mask:{:.3}, rt:{:.3}, intensity:{:.3}, reshape:{:.3})",
                 thread_id, precursor_data.precursor_id, local_timings.total,
                 local_timings.matrix_building, local_timings.ms1_extraction, local_timings.ms2_extraction,
                 local_timings.mask_building, local_timings.rt_extraction, local_timings.intensity_matrix,
                 local_timings.reshape_combine);
    }
    
    Ok((precursor_data.precursor_id.clone(), rsm_matrix, all_rt.to_vec()))
}

fn save_batch_results_as_npy(
    results: &[RSMPrecursorResults], 
    original_precursor_list: &[PrecursorLibData],
    batch_idx: usize,
    output_dir: &str,
) -> Result<(), Box<dyn Error>> {
    use std::io::Write;
    
    if results.is_empty() {
        return Err("No results to save".into());
    }
    
    // Create batch-specific filenames with simple numbering
    let batch_name = format!("batch_{}", batch_idx);
    let rsm_filename = format!("{}/{}_rsm.npy", output_dir, batch_name);
    let rt_filename = format!("{}/{}_rt_values.npy", output_dir, batch_name);
    let index_filename = format!("{}/{}_index.txt", output_dir, batch_name);
    
    // Create a map of index to result for quick lookup
    let mut result_map = std::collections::HashMap::new();
    for result in results {
        result_map.insert(result.index, result);
    }
    
    // Initialize the combined RSM matrix and RT values based on original order
    let n_original = original_precursor_list.len();
    let frag_repeat_num = 5;
    let n_fragments = 72; // MS1 + MS2 fragments
    let n_scans = 396;
    
    // Initialize with zeros
    let mut all_rsm_matrix = Array4::<f32>::zeros((n_original, frag_repeat_num, n_fragments, n_scans));
    let mut all_rt_values = Array2::<f32>::zeros((n_original, n_scans));
    let mut precursor_ids = Vec::with_capacity(n_original);
    let mut status_list = Vec::with_capacity(n_original);
    
    // Fill matrices in original order
    for (i, precursor_data) in original_precursor_list.iter().enumerate() {
        precursor_ids.push(precursor_data.precursor_id.clone());
        
        if let Some(result) = result_map.get(&i) {
            // Successfully processed - copy the RSM matrix
            all_rsm_matrix.slice_mut(s![i, .., .., ..]).assign(&result.rsm_matrix.slice(s![0, .., .., ..]));
            
            // Copy RT values
            for (j, &rt_val) in result.all_rt.iter().enumerate() {
                if j < n_scans {
                    all_rt_values[[i, j]] = rt_val;
                }
            }
            status_list.push("SUCCESS");
        } else {
            // Failed to process - keep as zeros
            status_list.push("FAILED");
        }
    }
    
    // Save RSM matrix
    println!("Saving RSM matrix to: {}", rsm_filename);
    println!("  Shape: [{}, {}, {}, {}]", n_original, frag_repeat_num, n_fragments, n_scans);
    write_npy(&rsm_filename, &all_rsm_matrix)?;
    
    // Save RT values
    println!("Saving RT values to: {}", rt_filename);
    println!("  Shape: [{}, {}]", n_original, n_scans);
    write_npy(&rt_filename, &all_rt_values)?;
    
    // Save index file
    println!("Saving index file to: {}", index_filename);
    let mut id_file = File::create(&index_filename)?;
    writeln!(id_file, "# Index file for RSM matrices and RT values - Batch {}", batch_idx)?;
    writeln!(id_file, "# Total precursors in batch: {}", n_original)?;
    writeln!(id_file, "# Successfully processed: {}", results.len())?;
    writeln!(id_file, "# Failed: {}", n_original - results.len())?;
    writeln!(id_file, "# RSM matrix shape: [{}, {}, {}, {}]", n_original, frag_repeat_num, n_fragments, n_scans)?;
    writeln!(id_file, "# RT values shape: [{}, {}]", n_original, n_scans)?;
    writeln!(id_file, "# Mode: {}", if SPEED_TEST_MODE { "SPEED TEST" } else { "FULL PROCESSING" })?;
    writeln!(id_file, "# Row_Index\tPrecursor_ID\tStatus")?;
    
    for (i, (id, status)) in precursor_ids.iter().zip(status_list.iter()).enumerate() {
        writeln!(id_file, "{}\t{}\t{}", i, id, status)?;
    }
    
    println!("\nSuccessfully saved batch {} files:", batch_idx);
    println!("  - RSM matrix: {}", rsm_filename);
    println!("  - RT values: {}", rt_filename);
    println!("  - Index file: {}", index_filename);
    
    Ok(())
}
