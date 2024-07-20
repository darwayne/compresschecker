# compresschecker
compress checker is a golang package that simplifies identifying compressed data in an efficient manner

## How efficient is it?
All operations use zero allocations and complete under 40 ns on a modern machine

## Supported Compression Formats
- zstd
- avro
- parquet
- xz
- rar
- zip
- gzip
- bzip2
- snappy
- 7zip

## Example Usage

### With a Reader

```go
package main

import (
	"fmt"
	"github.com/darwayne/compresschecker"
	"strings"
)

func main() {
	var stream = strings.NewReader("hello word")
	var reader = compresschecker.NewReadChecker(stream)
	defer reader.Close()
	if reader.IsCompressed() {
		fmt.Println("compression detected", reader.CompressionType())
	} else {
		fmt.Println("no compression detected")
    }
}
```

### With a string

```go
package main

import (
	"fmt"
	"github.com/darwayne/compresschecker"
	"strings"
)

func main() {
	var stream = "hello word"
	info := compresschecker.FormatOfString(stream)
	if info.IsCompressed() {
		fmt.Println("compression detected", info)
	} else {
		fmt.Println("no compression detected")
    }
}
```

### With a byte slice

```go
package main

import (
	"fmt"
	"github.com/darwayne/compresschecker"
)

func main() {
	var stream = []byte("hello word")
	info := compresschecker.FormatOfBytes(stream)
	if info.IsCompressed() {
		fmt.Println("compression detected", info)
	} else {
		fmt.Println("no compression detected")
	}
}
```

