
# TODO list

## Bugs

* value_t macro parsing for STR_GETRANGE should accept negative
  numbers, but does not.

## Technical debt and cleanups

* The protocol is binary buffers.  For simplicity, when running commands
  such as `sdiff`, multiple elements are output separated by a newline
  (\n).  This does not work with keys containing binary nuls and other
  non-display values.

* Create our own Error, and stop overloading io::Result

