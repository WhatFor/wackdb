use clap::Parser;
use env_logger::Env;
use repl::Repl;

use crate::server::{Server, ServerConfig};

mod executor;
mod grpc;
mod repl;
mod server;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short = 'd', long, value_name = "port")]
    daemon: Option<u16>,
}

fn init_logger() {
    let env = Env::default().default_filter_or("TRACE");

    env_logger::Builder::from_env(env)
        .format_target(false)
        .init();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logger();

    log::info!("Welcome to WackDB");
    log::info!("-----------------");

    let args = Args::parse();

    if let Some(port) = args.daemon {
        Server::new(ServerConfig::Grpc(port)).run().await?;
    } else {
        Repl::new().run();
    }

    Ok(())
}
