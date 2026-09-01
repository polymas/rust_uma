fn main() {
    println!("cargo:rerun-if-changed=proto/uma.proto");
    prost_build::compile_protos(&["proto/uma.proto"], &["proto"])
        .expect("compile UMA protobuf schema");
}
