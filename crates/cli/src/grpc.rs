use engine::engine::Engine;
use tonic::{Request, Response, Status};

use wack::query_service_server::QueryService;
use wack::{ExecuteRequest, ExecuteResponse, Row, Set};

use crate::executor;

pub mod wack {
    tonic::include_proto!("wack");
}

pub struct QueryServer {
    engine: Engine,
}

impl QueryServer {
    pub fn new(engine: Engine) -> Self {
        QueryServer { engine }
    }
}

#[tonic::async_trait]
impl QueryService for QueryServer {
    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        log::debug!("gRPC Request: {:?}", request);

        let query_result = executor::eval_command(&self.engine, &request.into_inner().sql);

        let response = match query_result {
            executor::CommandResult::_UnrecognisedCommand => todo!(),
            executor::CommandResult::ParseError(_) => todo!(),
            executor::CommandResult::ExecuteError(_) => todo!(),
            executor::CommandResult::Ok(statement_results) => {
                // TODO: This is one big map with a whole load of re-allocation (i assume). Can it be better? Yes. How? I don't know
                let sets = statement_results
                    .iter()
                    .map(|set| Set {
                        rows: set
                            .result_set
                            .rows
                            .iter()
                            .map(|r| Row {
                                values: r.iter().map(|v| v.to_string()).collect(),
                            })
                            .collect(),
                    })
                    .collect();

                ExecuteResponse { sets }
            }
        };

        Ok(Response::new(response))
    }
}
