### TODO List
- The schema info doesn't store which file a given database is in, so user DBs wont work atm
- the payload inserted into the wal isn't sorted - probably needs to be to read it back out
- validating if columns exist for a given table during insert (and maybe select?) is case sensitive.
- running an insert without specifying the PK column will bork the index atm, as the PK value default to the default defined in the schema.
- The lexer doesn't seem to do qualified identifiers, e.g.
  - SELECT * FROM master.databases;
  - INSERT INTO master.databases (Id) VALUES (1);
- The parser should check that the number of columns matches the number of values in an INSERT statement.
- I quite often just pass the file_manager through into the buffer_pool when it's needed; This seems pointless. Can't I just wire that up once?
- Implement support for writes (see next section).
- Implement a WAL.
- Need to update the buffer_pool; currently it just writes all pages straight to disk rather than batching and flushing. This is pretty shit.

### 17/04/2026 Daemon
- Try to use tokio with grpc; easy, well documented, writing a wire protocol isn't what this project is about.

Want to support two usage patterns:

- REPL: launch the CLI 'normally' and in process. Can send queries over the REPL, get back results. This is the 'dumb' version.
- Daemon: Can start the DB behind a gRPC listener via `wackdb -d 5000` to run on `localhost:5000`. At the  moment, won't bother with writing a CLI app that can send queries to the daemon, but probably wouldn't be too hard. (Okay maybe I'll do this). The typical usage will be via a long-lived connection from a client, using a package of some sort. For example, a .NET NuGet package, or an Entity Framework adapter.

### 18/04/2026 Planning a schema cache

- not sure what to call it; maybe just SchemaCache? lmao
- all it's info just comes from the master DB's SchemaInfo page, and then from the various master tables like Databases, Tables, etc. So just normal pages in normal DB files. This means it can and should all come from the buffer_pool. So this should sit on top of the BP.
- Will need a way to invalidate the cache; When the schema is changed, that's going to be through the buffer_pool but the SchemaCache won't know. I think this might mean schema changes should happen through this. Which means it might not be just a cache any more - it's a schema manager.
- It'll want to keep track of ALL schema info, not just master schema - user schema too.

### 19/04/2026 Supporting Writes
I'm trying to implement writes. I have added support into the lexer and parser for basic INSERT INTO statments, and the VM has a evaluate_insert_statement function with access to the &Storage context (granting access to the buffer_pool).
I know I need to do some initial validation: check the specified DB, table and columns all exist, but that's easy; I have the SchemaManager for that. We don't need to talk about that.

I'm a bit stumped on how I actually write to the DB. I can't support creating custom tables, so lets just focus on inserting into master.databases for now - this table is guaranteed to exist. The bootstrap process handles the initial creation of the table, by creating a B-tree index, initialising a number of Database structs, using deku to transform them into a byte array suitable for storage and pushing it into a slot in the index. It's very primitive, in that it won't handle stuff like re-balancing the tree or handling multiple pages of the index if one fills up. This is okay, as I know what I'm writing won't go beyond one page.
This isn't true now I'm trying to add support for INSERT statements.

I'm struggling to wrap my head around how inserts should work.
There's a couple things I know to be true:
- The insert needs to pass via the buffer_pool so I can update the in-memory representation of the page. The buffer_pool should probably mark the page as dirty so a future background task can come and find it to flush it to disk as a bulk operation. The buffer_pool's LRU should probably never evict dirty pages, as if we re-read them from disk it'll be out of date. This means if we do need to evict a dirty page, it needs to be flushed before doing so.
- If the buffer_pool doesn't have the page cached already, it should probably read it, update it, and mark it as dirty?
- The operation should also push the transaction into a WAL. This should happen before we update the buffer_pool, I think. This means if the system dies before the buffer_pool flushes dirty pages to disk, we still have all the completed transactions stored in the WAL and can recover.
- I don't know when a WAL is cleared down... Is it a strictly recovery operation? i.e. if the system boots and we have stuff in the WAL, do we need to recover and should handle all WAL transactions? and after that, the WAL should be empty? and for normal, happy operation of the system, the background service should come along, see the dirty pages in the buffer_pool, flush them to disk and... do what with the WAL? I'm quite unclear how the two interact.
- What the hell even is in a WAL? Does it record the queries? Or do we want to just record "wrote these bytes to this page and this offset"?
- Is the WAL paged? surely it's not an index, it's just an ordered, endless list of stuff.
- what header info do I need in the log files?
  - probably need:
    - magic string and version
    - last checkpoint location
    - last flushed location (just in case there's garbage data following the last flushed log)
- Postgres uses an 'LSN', log sequence number, which is an offset into a file.
- Postgres: each page in the buffer_pool tracks the LSN of the last log that modified it.
- What is each log record?

#### 19/04/2026 Some decisions
- WAL needs to write early, before the buffer_pool is updated. If we don't write the WAL, the txn failed and nothing else happens. Once the WAL is written, the page in the pool is marked dirty.
- The WAL is append-only (a checkpointing process can edit it, though).
- When checkpointing happens, we flush all dirty pages to disk from the buffer_pool, then append a 'flush_event' to the WAL. This signifies that everything before the flush_event is in disk and can be discarded (however I want to do that).
- During recovery, we find the most recent flush_event in the WAL and re-apply all following transactions.
- Each log is:
  - a header, including:
    - LSN (32-bit, although this means the log can't go past 4GB)
    - transaction ID (64-bit)
    - type (insert/update/delete/checkpoint/commit/whatever/etc)
    - payload len (how many bytes after the header before the next log)
    - checksum
    - the prev LSN? lets us walk back through the log, might be cool for undoing transactions?
  - a physiological log, including:
    - which file (though maybe don't need this as I'm not caring about multi-file per DB, and each DB gets a WAL)
    - which page in the file
    - and info about the actual data to write if an insert/update:
      - slot index
      - data len
      - data bytes
    - for a delete, only:
      - slot index
    - for commit or similar:
      - maybe just a timestamp? i dont think anything is strictly needed
    - for checkpoints:
      - ??? not thinking too much about this just yet, but I know I'll need something here. that's okay.

### Done List
- Add support for these two modes. Will need to support a cmdline argument to toggle daemon mode by `-d`. This will probably live in `cli`. Normally `-d` will imply a *background* daemon, but I don't care about that for now so executing the command will block. Maybe I'll look at supporting this later on. Invoking with `-d` will invoke a `server.rs` module.
- Install and setup tokio to run the server mode. For now, I'll ignore the REPL, but I have a feeling the tokio runtime will leak into the code and I'll need to support it in the REPL anyway. But I'd rather not, as the REPL is serial and gains nothing from tokio.
- Can I test this `server.rs` module? Figure that out. Might just be a simple start/stop harness? All it really does it receive requests and pass them to the parser/lexer, at which point that's all tested.
- Transition to sending all read/write traffic via the page_cache (or buffer_pool; maybe rename it?).
- Remove find_user_databases in bootstrap.rs; this data is in the DB now.
- Add an in-memory store for schema info; it changes so rarely that it doesn't make sense to read/decode it every query. This is used in vm.rs and in bootstrap.open_user_dbs.
- Add support for INSERT INTO statements into parser.
