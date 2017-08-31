[![GoDoc](https://godoc.org/github.com/steakknife/hamming?status.png)](https://godoc.org/github.com/steakknife/hamming)


# hamming distance calculations in Go

Copyright © 2014, 2015 Barry Allard

[MIT license](MIT-LICENSE.txt)

## Usage

```go
import 'github.com/steakknife/hamming'

// ...

// hamming distance between values
hamming.Byte(0xFF, 0x00) // 8
hamming.Byte(0x00, 0x00) // 0

// just count bits in a byte
hamming.CountBitsByte(0xA5), // 4
```

See help in the [docs](https://godoc.org/github.com/steakknife/hamming)

## Get

    go get -u github.com/steakknife/hamming  # master is always stable

## Source

- On the web: https://github.com/steakknife/hamming

- Git: `git clone https://github.com/steakknife/hamming`

## Contact

- [Feedback](mailto:barry.allard@gmail.com)

- [Issues](https://github.com/steakknife/hamming/issues)

## License 

[MIT license](MIT-LICENSE.txt)

Copyright © 2014, 2015 Barry Allard
