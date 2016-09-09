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

Example
--

```go
import "github.com/syndtr/goleveldb/leveldb"

func main() {
	leveldb, _ := leveldb.OpenFile("db", nil)
	defer leveldb.Close()
	db := NewCanalDB(leveldb)

	entry1, _ := db.Put("namespace", "1")
	entry2, _ := db.Put("namespace", "2")
	entry3, _ := db.Put("namespace", "3")

	db.GetCurrent("namespace") // => namespace:3

	db.GetRange("namespace", entry1.Timestamp, entry3.Timestamp, -1, false) // => namespace:1, namespace:2, namespace:3
	db.GetRange("namespace", entry1.Timestamp, entry3.Timestamp, -1, true)  // => namespace:3, namespace:2, namespace:1
	db.GetRange("namespace", entry1.Timestamp, entry3.Timestamp, 1, false)  // => namespace:1
	db.GetRange("namespace", entry2.Timestamp, entry3.Timestamp, -1, false) // => namespace:2, namespace:3

	db.Trim("namespace", entry3.Timestamp) // => Trim db
	db.GetRange("namespace", entry1.Timestamp, entry3.Timestamp, -1, false) // => namespace:3
}
```

Author
--

moznion (<moznion@gmail.com>)

License
--

MIT

