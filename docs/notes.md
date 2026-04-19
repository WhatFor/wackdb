### Daemon

- Try to use tokio with grpc; easy, well documented, writing a wire protocol isn't what this project is about.

Want to support two usage patterns:

- REPL: launch the CLI 'normally' and in process. Can send queries over the REPL, get back results. This is the 'dumb' version.
- Daemon: Can start the DB behind a gRPC listener via `wackdb -d 5000` to run on `localhost:5000`. At the  moment, won't bother with writing a CLI app that can send queries to the daemon, but probably wouldn't be too hard. (Okay maybe I'll do this). The typical usage will be via a long-lived connection from a client, using a package of some sort. For example, a .NET NuGet package, or an Entity Framework adapter.

### TODO List

- I quite often just pass the file_manager through into the buffer_pool when it's needed; This seems pointless. Can't I just wire that up once?
- Implement support for writes (see next section).
- Implement a WAL.
- Need to update the buffer_pool; currently it just writes all pages straight to disk rather than batching and flushing. This is pretty shit.

### Supporting Writes

- Check lexer and parser:
  - INSERT support
    - lexer seems fine, parser does not.
- Update VM to handle INSERTS.
- For now, just have the write go through the buffer_pool direct to disk.
- Think about the WAL, but don't implement yet.

### Done List

- Add support for these two modes. Will need to support a cmdline argument to toggle daemon mode by `-d`. This will probably live in `cli`. Normally `-d` will imply a *background* daemon, but I don't care about that for now so executing the command will block. Maybe I'll look at supporting this later on. Invoking with `-d` will invoke a `server.rs` module.
- Install and setup tokio to run the server mode. For now, I'll ignore the REPL, but I have a feeling the tokio runtime will leak into the code and I'll need to support it in the REPL anyway. But I'd rather not, as the REPL is serial and gains nothing from tokio.
- Can I test this `server.rs` module? Figure that out. Might just be a simple start/stop harness? All it really does it receive requests and pass them to the parser/lexer, at which point that's all tested.
- Transition to sending all read/write traffic via the page_cache (or buffer_pool; maybe rename it?).
- Remove find_user_databases in bootstrap.rs; this data is in the DB now.
- Add an in-memory store for schema info; it changes so rarely that it doesn't make sense to read/decode it every query. This is used in vm.rs and in bootstrap.open_user_dbs.

### Planning a schema cache

- not sure what to call it; maybe just SchemaCache? lmao
- all it's info just comes from the master DB's SchemaInfo page, and then from the various master tables like Databases, Tables, etc. So just normal pages in normal DB files. This means it can and should all come from the buffer_pool. So this should sit on top of the BP.
- Will need a way to invalidate the cache; When the schema is changed, that's going to be through the buffer_pool but the SchemaCache won't know. I think this might mean schema changes should happen through this. Which means it might not be just a cache any more - it's a schema manager.
- It'll want to keep track of ALL schema info, not just master schema - user schema too.
