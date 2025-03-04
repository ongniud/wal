# Write-Ahead Log (WAL)

This is a Write-Ahead Log (WAL) implementation in Go, designed to provide durable and efficient logging for data storage systems.

## Features

- Write data to disk with durability guarantees
- Read data back from disk efficiently
- Segment-based storage for managing large amounts of data

## Example

```go
package main

import (
    "fmt"
    "log"
    "time"
	
    "github.com/ongniud/wal"
)

func main() {
    // Open WAL
    w, err := wal.Open(wal.Options{
        Directory:    "/path/to/logs",
        SegmentSize:  1 * wal.GB,
        SyncInterval: 1 * time.Second,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer w.Close()

    // Write data
    data := []byte("hello, world")
    pos, err := w.Write(data)
    if err != nil {
        log.Fatal(err)
    }

    // Read data
    readData, err := w.Read(pos)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Read data: %s\n", string(readData))
}
```

## Contributing
Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.
## License
This project is licensed under the MIT License - see the LICENSE file for details.
