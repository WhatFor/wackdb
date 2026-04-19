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
        let engine = Engine::default();

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::wack::{query_service_client::QueryServiceClient, ExecuteRequest};
    use tokio::net::TcpListener;
    use tonic::transport::server::TcpIncoming;

    #[tokio::test]
    async fn test_grpc_query_server_responds() {
        // Bind to a random free port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));

        let engine = Engine::default();
        let query_server = QueryServer::new(engine);

        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(QueryServiceServer::new(query_server))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        let mut client = QueryServiceClient::connect(format!("http://{addr}"))
            .await
            .unwrap();

        let response = client
            .execute(ExecuteRequest {
                sql: "SELECT 1".into(),
            })
            .await;

        assert!(response.is_ok());
    }
}
