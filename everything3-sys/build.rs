use std::env;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let include_path = manifest_dir.join("include");
    let lib_path = manifest_dir.join("lib");

    // Determine library name based on target architecture
    let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();
    let lib_name = match target_arch.as_str() {
        "x86_64" => "Everything3_x64",
        "x86" => "Everything3_x86",
        "arm" => "Everything3_ARM",
        "aarch64" => "Everything3_ARM64",
        _ => panic!("Unsupported target architecture: {}", target_arch),
    };

    // Tell cargo to link the static library
    println!("cargo:rustc-link-search=native={}", lib_path.display());
    println!("cargo:rustc-link-lib=static={}", lib_name);
    println!("cargo:rustc-link-lib=ole32");

    // Tell cargo to rerun if these files change
    println!("cargo:rerun-if-changed=wrapper.h");
    println!(
        "cargo:rerun-if-changed={}",
        include_path.join("Everything3.h").display()
    );

    // Generate bindings
    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .clang_arg(format!("-I{}", include_path.display()))
        // Allow all Everything3 types and functions
        .allowlist_function("Everything3_.*")
        .allowlist_type("EVERYTHING3_.*")
        .allowlist_var("EVERYTHING3_.*")
        // Derive common traits
        .derive_debug(true)
        .derive_default(true)
        // Use core instead of std for no_std compatibility
        .use_core()
        // Generate layout tests
        .layout_tests(false)
        // Parse callbacks
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    // Write bindings to the $OUT_DIR/bindings.rs file
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
