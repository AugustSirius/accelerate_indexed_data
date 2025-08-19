#!/bin/bash
#SBATCH -p amd-ep2,intel-sc3,amd-ep2-short
#SBATCH -q normal
#SBATCH -J rust
#SBATCH -c 64
#SBATCH --mem 400G

# Load required modules
module load gcc

# Set Rust compiler optimizations WITHOUT LTO (let Cargo.toml handle it)
export RUSTFLAGS="-C target-cpu=znver2"

# Rayon will automatically use SLURM_CPUS_PER_TASK
export RAYON_NUM_THREADS=${SLURM_CPUS_PER_TASK}

# Enable memory optimizations
export MIMALLOC_LARGE_OS_PAGES=1

# Print job information
echo "========================================="
echo "Job started at: $(date)"
echo "Job ID: ${SLURM_JOB_ID}"
echo "Node: ${SLURM_NODELIST}"
echo "CPUs allocated: ${SLURM_CPUS_PER_TASK}"
echo "========================================="

cd /storage/guotiannanLab/wangshuaiyao/006.DIABERT_TimsTOF_Rust/accelerate_indexed_data/timstof-opt_build_indexed-opt_read-debug2

# Clean build to avoid cached issues
cargo clean

# Build and run with release profile
cargo build --release
time cargo run --release

echo "Job completed at: $(date)"