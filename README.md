Canaldb [![Build Status](https://travis-ci.org/moznion/Canaldb.svg?branch=master)](https://travis-ci.org/moznion/Canaldb)
==

TBD, just a toy!

A deadly simple time series databese which uses Leveldb as a backend.

Idea & Motivation
--

### Avoid storing a value which is duplicate of the most recent value

To save storage capacity.

e.g.

| Timestamp     | Value   | Note                   |
| ------------- | ------- | ---------------------- |
| 1473320409401 | foo     | Store (original value) |
| 1473320409402 | foo     | Not store (duplicated) |
| 1473320409403 | bar     | Store (value chaneged) |


### Trimmable (truncatable) old value with according to timestamp

TBD

Author
--

moznion (<moznion@gmail.com>)

License
--

MIT

