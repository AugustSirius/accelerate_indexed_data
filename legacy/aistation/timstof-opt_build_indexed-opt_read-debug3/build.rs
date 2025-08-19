// build.rs - Place this file in the project root directory
// This file is executed before compiling to set up optimizations

fn main() {
    // Detect CPU features at compile time
    println!("cargo:rerun-if-changed=build.rs");
    
    // Enable specific CPU optimizations based on target
    if cfg!(target_arch = "x86_64") {
        // Enable AVX2 if available
        println!("cargo:rustc-cfg=avx2");
        
        // Enable other x86_64 features
        println!("cargo:rustc-env=CARGO_CFG_TARGET_FEATURE=avx2,fma,bmi2");
    }
    
    // Set optimization flags for faster linking
    if cfg!(target_os = "linux") {
        // Use faster linker if available on Linux
        if std::process::Command::new("lld")
            .arg("--version")
            .output()
            .is_ok()
        {
            println!("cargo:rustc-link-arg=-fuse-ld=lld");
        }
    }
}