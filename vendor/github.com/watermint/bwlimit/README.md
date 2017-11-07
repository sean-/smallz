# bwlimit

[![Build Status](https://travis-ci.org/watermint/bwlimit.svg?branch=master)](https://travis-ci.org/watermint/bwlimit)
[![Coverage Status](https://coveralls.io/repos/github/watermint/bwlimit/badge.svg?branch=master)](https://coveralls.io/github/watermint/bwlimit?branch=master)

Limit IO bandwidth for multiple io.Reader/io.Writer.

## Usage

```go

func main() {
	// Limit to 100k Bytes per second
	bwlimit := NewBwlimit(100 * 1024, false)

	// Create io.Reader wrapper
	f1, _ := os.Open("data1.dat")
	fr1 := bwlimit.Reader(f1)

	f2, _ := os.Open("data2.dat")
	fr2 := bwlimit.Reader(f2)

	// Read loop
	for {
		buf := make([]byte, 100)
		n, err1 := fr1.Read(buf)
		n, err2 := fr2.Read(buf)

		// ...
	}

	// Wait for all reader close or reach EOF
	bwlimit.Wait()
}
```

## Internal

`Bwlimit` limits bandwidth by adjusting window size for each `io.Reader` or `io.Writer`. `Bwlimit` runs goroutine which invoke adjustment every takt time. Takt time is 10ms by default.
This mean if you setup `Bwlimit` as 100Mbytes per sec. Wrapped `Reader`/`Writer` could transmit 1Mbytes per 10ms. If your network router etc don't have enough buffer for hundle these traffic, please consider more higher fequencey like below.

```go
bwlimit := NewBwlimit(100 * 1024, false)
bwlimit.SetTaktPerSecond(1000)
```

In current implementation, above 1000 takt per second will be adjusted into 1000.

