use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=proto/uma.proto");
    println!("cargo:rerun-if-changed=.git/HEAD");
    prost_build::compile_protos(&["proto/uma.proto"], &["proto"])
        .expect("compile UMA protobuf schema");

    let commit = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=RUST_UMA_GIT_COMMIT={commit}");
}
