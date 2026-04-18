fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("BUILDING...");
    tonic_prost_build::compile_protos("proto/query.proto")?;
    Ok(())
}
