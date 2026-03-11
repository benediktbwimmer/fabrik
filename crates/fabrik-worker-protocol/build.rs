fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc is available");
    // SAFETY: build scripts run single-threaded per crate; setting PROTOC here only affects this build.
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/activity_worker.proto"], &["proto"])
        .expect("worker protocol compiles");
}
