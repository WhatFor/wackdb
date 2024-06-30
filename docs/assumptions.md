# Assumptions

This document lists the assumptions made for development, and what the ideal is based on that assumption. In a way, this is a tech debt log.

### Hardcoded DB Path

The program cannot store db files in arbitrary locations; The path to the DB files is hardcoded, and is `./data/`.

Calling `CREATE DATABASE test` will create the file `./data/test.wak`. This is a compromise for now because I don't want to add AST parsing for filepaths, so only a name can be specified. 🙂 It honestly wouldn't be too hard to add in a parameter to the `CREATE DATABASE` command that accepts a path as a string, but it's not needed for now.

The system will probably use a WAL, and that file will exist similarly to the main data file under `./data/`. For the `test` database example prior, this file will be `./data/test.wal`.

Ideally, this is controlled either by passing a filepath to the executable, or perhaps down the road controlled via a network protocol to connect to databases remotely. If the latter, that involves a lot more complexity as the program will be responsible for tracking the different DBs it knows about. To put this in real-world terms, SQLite vs Postgres/MySql/MSSql style. Regardless, it doesn't matter for development.
