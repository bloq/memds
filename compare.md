
# Comparison with redis commands

## Keys operations

 - [x] DEL
 - [ ] DUMP
 - [x] EXISTS
 - [ ] EXPIRE
 - [ ] EXPIREAT
 - [ ] KEYS
 - [ ] MIGATE
 - [ ] MOVE
 - [ ] OBJECT
 - [ ] PERSIST
 - [ ] PEXPIRE
 - [ ] PEXPIREAT
 - [ ] PTTL
 - [ ] RANDOMKEY
 - [ ] RENAME
 - [ ] RENAMENX
 - [ ] RESTORE
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
 - [ ] BGSAVE
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

