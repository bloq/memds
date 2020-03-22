# memds
Memory database - "redis v3, in Rust"

## Goals and Journey

memds intends to be "the next thing after redis"

Looking through software history, we can consider [memcached](https://memcached.org/) as Version 1:  memcached provides a single key/value namespace for string values, plus some small mods for atomic numbers.   [redis](https://redis.io/) is Version 2:  Differences between abstract data types (ADTs) are made explicit with strings, sets, lists, hash [tables], streams and more.

memds is Version 3:  Formally model the network protocol and database namespace.  Represent abstract data types as CLASS.METHOD internal Remote Procedure Calls (RPCs), batched together as a bytecode-like stream of database mutation operations.

## Model & design comparisons

### Model caveats

The current code is still a work in progress, in terms of implementing the models described below.   See the following markdown docs for more detailed information:
* [TODO](TODO.md)
* [Detailed redis feature comparison](compare.md)
* [Other project notes](notes.md)

### Old-vs-New

Old redis model:
```
     [database number] [key] [abstract data type]
```
New memds model:
```
     [database key] [key] [abstract data type, possibly with its own hierarchy]
```

* Old redis protocol hierarchy:  All ADTs overloaded in a single command namespace ("HLEN","LLEN").
* New memds protocol hierarchy:  Each ADT in its own class-specific namespace.  (like "HASH.LEN","LIST.LEN", but with integer identifiers).

* Old redis network protocol:  Custom protocol, requiring custom clients across N programming languages.
* New memds network protocol:  Protobuf schema, automatically generating correct, compliant, fast client codecs for many languages.

## Components

* `memds-cli`: Command line client
* `memds-proto`: wire protocol library
* `memds-server`: Database server

## Installation

All building is done via the standard [Rust](https://www.rust-lang.org/) tool [Cargo](https://doc.rust-lang.org/cargo/).

```
$ cargo build --release
$ cargo test --release
```
