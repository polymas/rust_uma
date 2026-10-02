use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=proto/polyuma/wire/v1/uma.proto");
    println!("cargo:rerun-if-changed=proto/polyuma/forfeit/v1/forfeit.proto");
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/logs/HEAD");
    assert!(
        std::path::Path::new("proto/polyuma/wire/v1/uma.proto").exists(),
        "proto/ 是 git submodule（polymas/proto），先执行 `git submodule update --init`"
    );
    prost_build::compile_protos(
        &[
            "proto/polyuma/wire/v1/uma.proto",
            // forfeit-feed 推送格式：rust-uma 订阅它维护弃权排除名单（src/forfeit.rs）
            "proto/polyuma/forfeit/v1/forfeit.proto",
        ],
        &["proto"],
    )
    .expect("compile protobuf schemas");

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
