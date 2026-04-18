# WackDB CLI

### Daemon mode

```sh
cargo run -d 1234
```

```sh
grpcurl -plaintext -import-path ./crates/cli/proto -proto query.proto -d '{"sql": "SELECT 1;"}' '[::1]:1234' wack.QueryService/Execute
```
