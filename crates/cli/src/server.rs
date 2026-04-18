use engine::engine::Engine;

use crate::grpc::wack::query_service_server::QueryServiceServer;
use crate::grpc::QueryServer;

type PortNumber = u16;

pub enum ServerConfig {
    Grpc(PortNumber),
}

pub struct Server {
    config: ServerConfig,
}

impl Server {
    pub fn new(config: ServerConfig) -> Self {
        Server { config }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut engine = Engine::default();
        engine.init();

        match self.config {
            ServerConfig::Grpc(port_number) => {
                log::info!("Starting WackDB Server on port {}...", port_number);

                let addr_str = format!("[::1]:{}", port_number);
                let addr = addr_str.parse()?;

                let query_server = QueryServer::new(engine);

                tonic::transport::Server::builder()
                    .add_service(QueryServiceServer::new(query_server))
                    .serve(addr)
                    .await?;

                Ok(())
            }
        }
    }
}
