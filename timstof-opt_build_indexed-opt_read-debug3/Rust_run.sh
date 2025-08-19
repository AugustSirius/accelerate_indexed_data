#!/bin/bash
#SBATCH -p amd-ep2,intel-sc3,amd-ep2-short
#SBATCH -q normal
#SBATCH -J rust
#SBATCH -c 64
#SBATCH --mem 400G

# module
module load gcc
cd /storage/guotiannanLab/wangshuaiyao/006.DIABERT_TimsTOF_Rust/accelerate_indexed_data/timstof-opt_build_indexed-opt_read-debug3
cargo run --release

# # Detect CPU architecture and set appropriate flags
# if lscpu | grep -q "AMD EPYC"; then
#     # AMD EPYC optimizations (likely Zen 2 or Zen 3)
#     if lscpu | grep -q "7742\|7763"; then
#         # Zen 2 or Zen 3
#         export RUSTFLAGS="-C target-cpu=znver3 -C opt-level=3 -C lto=fat -C embed-bitcode=yes -C codegen-units=1"
#     else
#         # Generic AMD EPYC
#         export RUSTFLAGS="-C target-cpu=znver2 -C opt-level=3 -C lto=fat -C embed-bitcode=yes -C codegen-units=1"
#     fi
# elif lscpu | grep -q "Intel"; then
#     # Intel optimizations
#     export RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C lto=fat -C embed-bitcode=yes -C codegen-units=1"
# else
#     # Generic optimizations
#     export RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C lto=fat -C embed-bitcode=yes -C codegen-units=1"
# fi

# # Enable huge pages for better memory performance
# export MIMALLOC_LARGE_OS_PAGES=1
# export MIMALLOC_RESERVE_HUGE_OS_PAGES=8

# # Set thread affinity for better cache usage
# export RAYON_NUM_THREADS=${SLURM_CPUS_PER_TASK}
# export OMP_PROC_BIND=close
# export OMP_PLACES=cores

# # Memory allocator tuning
# export MIMALLOC_EAGER_COMMIT_DELAY=100
# export MIMALLOC_RESET_DELAY=100

# # Print job information
# echo "========================================="
# echo "Job started at: $(date)"
# echo "Job ID: ${SLURM_JOB_ID}"
# echo "Node: ${SLURM_NODELIST}"
# echo "CPUs allocated: ${SLURM_CPUS_PER_TASK}"
# echo "CPU Architecture: $(lscpu | grep 'Model name' | cut -d: -f2)"
# echo "RUSTFLAGS: ${RUSTFLAGS}"
# echo "========================================="

# cd /storage/guotiannanLab/wangshuaiyao/006.DIABERT_TimsTOF_Rust/accelerate_indexed_data/timstof-opt_build_indexed-opt_read-debug3

# # Clean build to ensure optimizations are applied
# echo "Cleaning previous build..."
# cargo clean

# # Build with maximum optimizations
# echo "Building with release profile..."
# cargo build --release

# # Check if build was successful
# if [ $? -ne 0 ]; then
#     echo "Build failed!"
#     exit 1
# fi

# # Run with performance monitoring
# echo "Starting performance run..."
# echo "========================================="

# # Run with time measurement
# /usr/bin/time -v cargo run --release 2>&1 | tee performance_log.txt

# # Extract and display key metrics
# echo "========================================="
# echo "Performance Summary:"
# grep -E "User time|System time|Elapsed|Maximum resident" performance_log.txt

# echo "========================================="
# echo "Job completed at: $(date)"