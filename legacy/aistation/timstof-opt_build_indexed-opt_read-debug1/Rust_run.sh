#!/bin/bash
#slurm options
#SBATCH -p amd-ep2,intel-sc3,amd-ep2-short
#SBATCH -q normal
#SBATCH -J rust
#SBATCH -c 1
#SBATCH -n 64
#SBATCH --mem 400G
########################## MSConvert run #####################
# module
module load gcc
export RUSTFLAGS="-C target-cpu=znver2"  # ADD THIS LINE!
cd /storage/guotiannanLab/wangshuaiyao/006.DIABERT_TimsTOF_Rust/accelerate_indexed_data/timstof-opt_build_indexed-opt_read-debug1
cargo run --release