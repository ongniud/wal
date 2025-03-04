# SlicePool

`SlicePool` is a generic slice pool implementation in Go that aims to manage slices of different sizes efficiently by reusing them. This helps reduce memory allocation and deallocation overhead, especially in scenarios where slices are frequently created and discarded.

## Features
- **Generic Support**: It supports slices of any type thanks to Go's generics feature.
- **Customizable Sizes**: You can define the minimum and maximum slice sizes, as well as the growth factor between sizes when creating the pool.
- **Efficient Allocation and Deallocation**: Utilizes `sync.Pool` to quickly allocate and release slices, improving performance.
- **Memory Leak Prevention**: Ensures that slices are properly reset before being put back into the pool to avoid potential memory leaks.

## Installation
Since this is a pure Go package, you can simply copy the code into your project or use it as part of a Go module. There are no external dependencies other than the standard Go library.

## Usage Example
```go
package main

import (
    "fmt"
    "github.com/ongniud/slice_pool"
)

func main() {
    // Create a SlicePool with default configuration
    p := pool.NewSlicePoolDefault[int]()

    // Allocate a slice of size 32
    size := 32
    slice := p.Alloc(size)
    fmt.Printf("Allocated slice of capacity %d\n", cap(slice))

    // Use the slice...

    // Return the slice to the pool
    p.Free(slice)
    fmt.Println("Slice freed and returned to the pool")
}
```

## Code Explanation

### Constants
- `minBufferSize`: The default minimum buffer size for the slice pool.
- `maxBufferSize`: The default maximum buffer size for the slice pool.
- `growFactor`: The default growth factor used to generate different slice sizes in the pool.

### Structs
- `SlicePool[T any]`: Represents the slice pool. It contains an array of slice sizes (`sizes`), an array of `sync.Pool` instances (`pools`), and the minimum and maximum slice sizes (`min` and `max`).

### Functions
- `NewSlicePoolDefault[T any]() *SlicePool[T]`: Creates a `SlicePool` with default configuration.
- `NewSlicePool[T any](min, max, factor int) *SlicePool[T]`: Creates a `SlicePool` with custom minimum, maximum, and growth factor values.
- `(p *SlicePool[T]) Alloc(size int) []T`: Allocates a slice of the specified size from the pool. If the requested size is not in the pool, it creates a new slice.
- `(p *SlicePool[T]) Free(slc []T)`: Returns a slice to the pool. Before returning, it resets the slice to avoid memory leaks.

## Unit Tests
Unit tests are provided to ensure the correctness of the `SlicePool` implementation. You can run the tests using the following command:
```sh
go test
```

The tests cover the following aspects:
- Creating a `SlicePool` with default and custom configurations.
- Allocating slices of specified sizes.
- Returning slices to the pool and verifying their reuse.

## Contribution
If you find any bugs or have suggestions for improvement, please open an issue or submit a pull request on the project repository.

## License
This project is open - source. You can use, modify, and distribute it under the terms of an appropriate open - source license (e.g., MIT License). Make sure to check the specific license details in the project. 
