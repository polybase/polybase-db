fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out = std::env::var("OUT_DIR").unwrap();
    println!("out: {}", out);
    let build_res = tonic_build::configure()
        // .out_dir(out)
        .compile(&["network.proto"], &["proto/"]);
    println!("compile proto result! {:?}", build_res);
    println!("cargo:rerun-if-changed=proto/network.proto");
    build_res.unwrap();
    Ok(())
}
