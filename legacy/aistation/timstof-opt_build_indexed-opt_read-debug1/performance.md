Initialized parallel processing with 64 threads
Detected OS: linux
Using macOS paths: false
Using data folder: /storage/guotiannanLab/wangshuaiyao/006.DIABERT_TimsTOF_Rust/test_data/CAD20220207yuel_TPHP_DIA_pool1_Slot2-54$
Using library file: /storage/guotiannanLab/wangshuaiyao/777.library/TPHPlib_frag1025_swissprot_final_all_from_Yueliang.tsv
Using report file: /storage/guotiannanLab/wangshuaiyao/006.DIABERT_TimsTOF_Rust/test_data/report.parquet

========== DATA PREPARATION PHASE ==========
Initializing metadata readers...
  Metadata initialization: 0.209s
Initializing frame reader...
  Frame reader initialization: 0.065s
  Total frames to process: 53168
Estimating data size for pre-allocation...
  Estimated MS1 peaks: ~473552289
Processing frames with hybrid optimizations (64 threads)...
  Frame processing: 130.545s
  Frames processed: 53168
Finalizing data structures with zero-copy merge...
  Data finalization: 45.951s

========== Data Summary ==========
MS1 data points: 591487824
MS2 windows: 32
MS2 data points: 2034948969
Total processing time: 176.950s
Raw data reading time: 176.97240 seconds
  - MS1 data points: 591487824
  - MS2 windows: 32

Building indexed data structures...
Building indexed data: MS1 points=591487824, MS2 windows=32
Index building time: 35.70187 seconds
Total data preparation time: 212.67508 seconds

========== LIBRARY AND REPORT PROCESSING ==========
Reading library file: /storage/guotiannanLab/wangshuaiyao/777.librar