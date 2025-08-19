Initialized parallel processing with 64 threads Detected OS: linux Using macOS paths: false Using data folder: /storage/guotiannanLab/wangshuaiyao/006.DIABERT_TimsTOF_Rust/test_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54_1_4382.d Using library file: /storage/guotiannanLab/wangshuaiyao/777.library/TPHPlib_frag1025_swissprot_final_all_from_Yueliang.tsv Using report file: /storage/guotiannanLab/wangshuaiyao/006.DIABERT_TimsTOF_Rust/test_data/report.parquet
========== DATA PREPARATION PHASE ========== Initializing metadata readers... Metadata initialization: 0.200s Initializing frame reader... Frame reader initialization: 0.064s Total frames to process: 53168 Estimating data size for pre-allocation... Estimated MS1 peaks: ~4868952048 Estimated MS2 peaks: ~1415084000 Processing frames with optimized pipeline (64 threads)... Frame processing: 182.727s Frames processed: 53168 Finalizing data structures with zero-copy merge... Data finalization: 11.781s
========== Data Summary ========== MS1 data points: 591487824 MS2 windows: 32 MS2 data points: 2034948969 Total processing time: 199.839s Raw data reading time: 200.17963 seconds
* MS1 data points: 591487824
* MS2 windows: 32
Building indexed data structures... Building indexed data: MS1 points=591487824, MS2 windows=32
Building MS1 index...
Building MS2 indices...
Index building completed in 38.236s
Index building time: 38.23605 seconds
Total data preparation time: 238.41576 seconds