fn main() {
    prost_build::compile_protos(
        &["proto/merkledag.proto", "proto/unixfs.proto"],
        &["proto/"],
    )
    .expect("Failed to compile protobuf definitions");
}
