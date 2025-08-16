mod utils;
mod cache;
mod processing;

use cache::CacheManager;
use utils::{
    read_timstof_data, build_indexed_data, read_parquet_with_polars,
    library_records_to_dataframe, merge_library_and_report, get_unique_precursor_ids, 
    process_library_fast, create_rt_im_dicts, build_lib_matrix, build_precursors_matrix_step1, 
    build_precursors_matrix_step2, build_range_matrix_step3, build_precursors_matrix_step3, 
    build_frag_info, LibCols, PrecursorLibData, prepare_precursor_lib_data
};
use processing::{
    FastChunkFinder, build_intensity_matrix_optimized, prepare_precursor_features,
    calculate_mz_range, extract_ms2_data, build_mask_matrices, extract_aligned_rt_values,
    reshape_and_combine_matrices, create_final_dataframe, process_single_precursor
};

use rayon::prelude::*;
use std::{error::Error, path::Path, time::Instant, env, fs::File};
use ndarray::{Array2, Array3, Array4, s, Axis};
use polars::prelude::*;

fn main() -> Result<(), Box<dyn Error>> {
    // Configurable parallel processing parameter
    let parallel_threads = 64; // Set to 1 for sequential, 2+ for parallel processing
    
    // Initialize global thread pool based on parallel_threads setting
    if parallel_threads > 1 {
        rayon::ThreadPoolBuilder::new()
            .num_threads(parallel_threads)
            .build_global()
            .unwrap();
        println!("Initialized parallel processing with {} threads", parallel_threads);
    } else {
        // For sequential processing, still initialize rayon with 1 thread
        rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build_global()
            .unwrap();
        println!("Running in sequential mode (1 thread)");
    }
    
    let args: Vec<String> = env::args().collect();
    
    // Handle command-line arguments for cache operations
    if let Some(arg) = args.get(1) {
        match arg.as_str() {
            "--clear-cache" => {
                CacheManager::new().clear_cache()?;
                return Ok(());
            }
            "--cache-info" => {
                let cache_manager = CacheManager::new();
                let info = cache_manager.get_cache_info()?;
                if info.is_empty() {
                    println!("Cache is empty");
                } else {
                    println!("Cache files:");
                    for (name, _, size_str) in info {
                        println!("  {} - {}", name, size_str);
                    }
                }
                return Ok(());
            }
            _ => {}
        }
    }
    
    // Automatic OS detection and file path assignment
    let is_macos = std::env::consts::OS == "macos";
    println!("Detected OS: {}", std::env::consts::OS);
    println!("Using macOS paths: {}", is_macos);
    
    // Set file paths based on OS detection
    let (default_data_folder, lib_file_path, report_file_path) = if is_macos {
        // macOS paths
        (
            "/Users/augustsirius/Desktop/DIA_peak_group_extraction/输入数据文件/raw_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d".to_string(),
            "/Users/augustsirius/Desktop/DIA_peak_group_extraction/输入数据文件/raw_data/TPHPlib_frag1025_swissprot_final_all_from_Yueliang.tsv",
            "/Users/augustsirius/Desktop/DIA_peak_group_extraction/输入数据文件/raw_data/report.parquet"
        )
    } else {
        // Non-macOS paths (original paths)
        (
            "/storage/guotiannanLab/wangshuaiyao/006.DIABERT_TimsTOF_Rust/test_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d".to_string(),
            "/storage/guotiannanLab/wangshuaiyao/777.library/TPHPlib_frag1025_swissprot_final_all_from_Yueliang.tsv",
            "/storage/guotiannanLab/wangshuaiyao/006.DIABERT_TimsTOF_Rust/test_data/report.parquet"
        )
    };
    
    // Set data folder path (can still be overridden by command line argument)
    let d_folder = args.get(1).cloned().unwrap_or(default_data_folder);
    
    let d_path = Path::new(&d_folder);
    if !d_path.exists() {
        return Err(format!("folder {:?} not found", d_path).into());
    }
    
    println!("Using data folder: {}", d_folder);
    println!("Using library file: {}", lib_file_path);
    println!("Using report file: {}", report_file_path);
    
    // ================================ DATA LOADING AND INDEXING ================================

    println!("\n========== DATA PREPARATION PHASE ==========");
    let total_start = Instant::now();

    // Read raw data
    let raw_data_start = Instant::now();
    let raw_data = read_timstof_data(d_path)?;
    println!("Raw data reading time: {:.5} seconds", raw_data_start.elapsed().as_secs_f32());
    println!("  - MS1 data points: {}", raw_data.ms1_data.mz_values.len());
    println!("  - MS2 windows: {}", raw_data.ms2_windows.len());
    
    // Build indexed data
    println!("\nBuilding indexed data structures...");
    let index_start = Instant::now();
    let (ms1_indexed, ms2_indexed_pairs) = build_indexed_data(raw_data)?;
    println!("Index building time: {:.5} seconds", index_start.elapsed().as_secs_f32());

    println!("Total data preparation time: {:.5} seconds", total_start.elapsed().as_secs_f32());
    
    // Create MS2 finder for fast chunk lookup
    let finder = FastChunkFinder::new(ms2_indexed_pairs)?;
    
    // ================================ LIBRARY AND REPORT LOADING ================================
    println!("\n========== LIBRARY AND REPORT PROCESSING ==========");
    let lib_processing_start = Instant::now();
    
    let library_records = process_library_fast(lib_file_path)?;
    let library_df = library_records_to_dataframe(library_records.clone())?;
    
    let report_df = read_parquet_with_polars(report_file_path)?;
    
    let diann_result = merge_library_and_report(library_df, report_df)?;
    let diann_precursor_id_all = get_unique_precursor_ids(&diann_result)?;
    let (assay_rt_kept_dict, assay_im_kept_dict) = create_rt_im_dicts(&diann_precursor_id_all)?;
    
    println!("Library and report processing time: {:.5} seconds", lib_processing_start.elapsed().as_secs_f32());
    
    // Set processing parameters
    let device = "cpu";
    let frag_repeat_num = 5;
    
    // ================================ BATCH PRECURSOR PROCESSING ================================
    println!("\n========== BATCH PRECURSOR PROCESSING ==========");
    
    // Step 1: Prepare library data first
    println!("\n[Step 1] Preparing library data for batch processing");
    let prep_start = Instant::now();
    
    // 获取unique precursor IDs
    let unique_precursor_ids: Vec<String> = diann_precursor_id_all
        .column("transition_group_id")?
        .str()?
        .into_iter()
        .filter_map(|opt| opt.map(|s| s.to_string()))
        .collect();
    
    let lib_cols = LibCols::default();
    let max_precursors = 800; // 可以根据需要调整
    
    // 预先构建所有precursor的library data
    let precursor_lib_data_list = prepare_precursor_lib_data(
        &library_records,
        &unique_precursor_ids,
        &assay_rt_kept_dict,
        &assay_im_kept_dict,
        &lib_cols,
        max_precursors,
    )?;
    
    println!("  - Prepared data for {} precursors", precursor_lib_data_list.len());
    println!("  - Preparation time: {:.5} seconds", prep_start.elapsed().as_secs_f32());
    
    // 释放library_records的内存，因为我们已经不需要它了
    drop(library_records);
    println!("  - Released library_records from memory");
    
    // Step 2: Process each precursor sequentially (可以后续改为并行)
    println!("\n[Step 2] Processing individual precursors");
    
    // 创建输出目录
    let output_dir = "output_precursors";
    std::fs::create_dir_all(output_dir)?;
    
    let device = "cpu";
    let frag_repeat_num = 5;

    let batch_start = Instant::now();
    
    // Process precursors based on parallel_threads setting
    if parallel_threads == 1 {
        // Sequential processing
        println!("Processing precursors sequentially...");
        for (idx, precursor_data) in precursor_lib_data_list.iter().enumerate() {
            println!("\n--- Processing precursor {}/{} ---", idx + 1, precursor_lib_data_list.len());
            
            match process_single_precursor(
                precursor_data,
                &ms1_indexed,
                &finder,
                frag_repeat_num,
                device,
                output_dir,
            ) {
                Ok(_) => {
                    println!("✓ Successfully processed: {}", precursor_data.precursor_id);
                },
                Err(e) => {
                    eprintln!("✗ Error processing {}: {}", precursor_data.precursor_id, e);
                }
            }
        }
    } else {
        // Parallel processing
        println!("Processing precursors in parallel with {} threads...", parallel_threads);
        
        // Use atomic counter for progress tracking in parallel mode
        use std::sync::atomic::{AtomicUsize, Ordering};
        let processed_count = AtomicUsize::new(0);
        let total_count = precursor_lib_data_list.len();
        
        // Process in parallel using rayon
        precursor_lib_data_list.par_iter().for_each(|precursor_data| {
            let result = process_single_precursor(
                precursor_data,
                &ms1_indexed,
                &finder,
                frag_repeat_num,
                device,
                output_dir,
            );
            
            // Update progress counter
            let current = processed_count.fetch_add(1, Ordering::SeqCst) + 1;
            
            match result {
                Ok(_) => {
                    println!("[{}/{}] ✓ Successfully processed: {}", 
                             current, total_count, precursor_data.precursor_id);
                },
                Err(e) => {
                    eprintln!("[{}/{}] ✗ Error processing {}: {}", 
                              current, total_count, precursor_data.precursor_id, e);
                }
            }
        });
    }
    
    let batch_elapsed = batch_start.elapsed();
    println!("\n========== BATCH PROCESSING SUMMARY ==========");
    println!("Processing mode: {}", if parallel_threads == 1 { "Sequential".to_string() } else { format!("Parallel ({} threads)", parallel_threads) });
    println!("Total batch processing time: {:.5} seconds", batch_elapsed.as_secs_f32());
    println!("Average time per precursor: {:.5} seconds", 
             batch_elapsed.as_secs_f32() / precursor_lib_data_list.len() as f32);
    
    Ok(())
}
