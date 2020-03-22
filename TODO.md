
# To-do

## Architecture

 - [ ] Convert from dumb "big match stmt" dispatch method to an operation
   call/return sequence that calls MODULE.METHOD, with modules
   registering their list of methods.

## Protocol

 - [ ] Adjunct/Task under MODULE.METHOD architecture to-do:  Strongly
 consider a "tighter" + more flexible encoding for module calls:

```
	module: uint32,
	method: uint32,
	enum Params
		value_list: Vec<bytes>
		pair_list: Vec<(bytes,bytes)>
```

  then re-introduce a schema checking (module.method+params validation)
  definition that restores what we lose by departing from protobufs a bit.

 - [ ] Protocol return values echo HTTP for want of a better practice.
 Update code to a better practice.

## Bugs

 - [ ] BGSAVE leaves zombie processes.  Need to wait(2)

## Code organization and Q/A

 - [ ] Reduce amount of boilerplate code in per-operation processing.
 - [ ] Improve per-op tests beyond it-works
 - [ ] CLI integration tests

