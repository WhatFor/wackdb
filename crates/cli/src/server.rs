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

    pub fn run(&mut self) {
        match self.config {
            ServerConfig::Grpc(port_number) => {
                log::info!("Starting WackDB Server on port {}...", port_number);
            }
        };
    }
}
