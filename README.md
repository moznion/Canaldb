Canaldb [![Build Status](https://travis-ci.org/moznion/Canaldb.svg?branch=master)](https://travis-ci.org/moznion/Canaldb)
==

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

To truncate old data, to save storage capacity :)

e.g.

| namespace and timestamp  | Value   |
| ------------------------ | ------- |
| ns_1473320409401         | foo     |
| ns_1473320409402         | bar     |
| ns_1473320409403         | buz     |
| ns_1473320409410         | qux     |

Then execute `Trim()`

```go
db.Trim("ns", int64(1473320409405)) // <= timestamp is between `buz` value and `qux` value
```

Result:

| namespace and timestamp  | Value   | Note                                          |
| ------------------------ | ------- | --------------------------------------------- |
| ns_1473320409405         | buz     | Truncate and put the latest value on boundary |
| ns_1473320409410         | qux     | As it is                                      |

Author
--

moznion (<moznion@gmail.com>)

License
--

MIT

