
# Comparison with redis

## General features

 - [x] Component: Memory database server
 - [x] Component: Command-line client
 - [ ] ADT: HashMap
 - [x] ADT: Lists
 - [ ] ADT: LRU cache
 - [x] ADT: Sets
 - [x] ADT: Strings
 - [x] I/O: Fork and dump to fs
 - [x] I/O: Import dump
 - [ ] I/O: Write-ahead logging
 - [ ] Memory limits
 - [ ] Network: Clusters
 - [ ] Statistics

## Keys operations

 - [x] DEL
 - [x] DUMP
 - [x] EXISTS
 - [ ] EXPIRE
 - [ ] EXPIREAT
 - [ ] KEYS
 - [ ] MIGRATE
 - [ ] MOVE
 - [ ] OBJECT
 - [ ] PERSIST
 - [ ] PEXPIRE
 - [ ] PEXPIREAT
 - [ ] PTTL
 - [ ] RANDOMKEY
 - [x] RENAME
 - [x] RENAMENX
 - [x] RESTORE
 - [ ] SORT
 - [ ] TOUCH
 - [ ] TTL
 - [x] TYPE
 - [ ] UNLINK
 - [ ] WAIT
 - [ ] SCAN

## List operations

 - [ ] BLPOP
 - [ ] BRPOP
 - [ ] BRPOPLPUSH
 - [x] LINDEX
 - [ ] LINSERT
 - [x] LLEN
 - [x] LPOP
 - [x] LPUSH
 - [x] LPUSHX
 - [ ] LRANGE
 - [ ] LREM
 - [ ] LSET
 - [ ] LTRIM
 - [x] RPOP
 - [ ] RPOPLPUSH
 - [x] RPUSH
 - [x] RPUSHX

## Server operations

 - [ ] BGREWRITEAOF
 - [x] BGSAVE
 - [ ] CLIENT ID
 - [ ] CLIENT KILL
 - [ ] CLIENT LIST
 - [ ] CLIENT GETNAME
 - [ ] CLIENT PAUSE
 - [ ] CLIENT REPLY
 - [ ] CLIENT SETNAME
 - [ ] CLIENT UNBLOCK
 - [ ] COMMAND
 - [ ] COMMAND COUNT
 - [ ] COMMAND GETKEYS
 - [ ] COMMAND INFO
 - [ ] CONFIG GET
 - [ ] CONFIG REWRITE
 - [ ] CONFIG SET
 - [ ] CONFIG RESETSTAT
 - [x] DBSIZE
 - [ ] DEBUG OBJECT
 - [ ] DEBUG SEGFAULT
 - [x] FLUSHALL
 - [x] FLUSHDB
 - [ ] INFO
 - [ ] LOLWUT
 - [ ] LASTSAVE
 - [ ] MEMORY DOCTOR
 - [ ] MEMORY HELP
 - [ ] MEMORY MALLOC-STATS
 - [ ] MEMORY PURGE
 - [ ] MEMORY STATS
 - [ ] MEMORY USAGE
 - [ ] MODULE LIST
 - [ ] MODULE LOAD
 - [ ] MODULE UNLOAD
 - [ ] MONITOR
 - [ ] ROLE
 - [ ] SAVE
 - [ ] SHUTDOWN
 - [ ] SLAVEOF
 - [ ] REPLICAOF
 - [ ] SLOWLOG
 - [ ] SYNC
 - [ ] PSYNC
 - [x] TIME
 - [ ] LATENCY DOCTOR
 - [ ] LATENCY GRAPH
 - [ ] LATENCY HISTORY
 - [ ] LATENCY LATEST
 - [ ] LATENCY RESET
 - [ ] LATENCY HELP

## Set operations

 - [x] SADD
 - [x] SCARD
 - [x] SDIFF
 - [x] SDIFFSTORE
 - [ ] SINTER
 - [ ] SINTERSTORE
 - [x] SISMEMBER
 - [x] SMEMBERS
 - [ ] SMOVE
 - [ ] SPOP
 - [ ] SRANDMEMBER
 - [x] SREM
 - [x] SUNION
 - [x] SUNIONSTORE
 - [ ] SSCAN

## String operations

 - [x] APPEND
 - [ ] BITCOUNT
 - [ ] BITFIELD
 - [ ] BITOP
 - [ ] BITPOS
 - [x] DECR
 - [x] DECRBY
 - [x] GET
 - [ ] GETBIT
 - [x] GETRANGE
 - [x] GETSET
 - [x] INCR
 - [x] INCRBY
 - [ ] INCRBYFLOAT
 - [x] MGET
 - [x] MSET
 - [ ] MSETNX
 - [ ] PSETEX
 - [x] SET
	- [ ] SET options, notably expiration
 - [ ] SETBIT
 - [ ] SETEX
 - [x] SETNX
 - [ ] SETRANGE
 - [x] STRLEN

## Transactions

 - [x] DISCARD
 - [x] EXEC
 - [x] MULTI
 - [ ] UNWATCH
 - [ ] WATCH

