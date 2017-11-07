package bwlimit

import (
	"io"
	"math"
	"sync"
	"time"
)

const (
	DEFAULT_TAKT_PER_SECOND = 100
	RATE_UNLIMITED          = 0
)

// Create new bandwidth limiter.
// rateLimit: maximum bandwidth in bytes per second. 0 for unlimited.
// blocking: block `Read`/`Write` until next takt time.
func NewBwlimit(rateLimit int, blocking bool) Bwlimit {
	bw := Bwlimit{
		streamWindow: make(map[uint64]int),
	}
	bw.SetTaktPerSecond(DEFAULT_TAKT_PER_SECOND)
	bw.SetRateLimit(rateLimit)
	bw.SetBlocking(blocking)

	return bw
}

// Bandwidth limiter.
type Bwlimit struct {
	// Rate limit in bytes per second
	rateLimit       int
	ratePerTaktTime int

	taktTime      int
	taktPerSecond int

	manualTakt bool
	blocking   bool

	streamSeq    uint64
	streamWindow map[uint64]int
	streamMutex  sync.Mutex

	timerRunning  bool
	timerShutdown sync.WaitGroup
	activeIO      sync.WaitGroup
}

// Set blocking
func (bw *Bwlimit) SetBlocking(blocking bool) {
	bw.blocking = blocking
}

// Set new takt count per second.
func (bw *Bwlimit) SetTaktPerSecond(taktPerSecond int) {
	if taktPerSecond >= 1 && taktPerSecond <= 1000 {
		bw.taktTime = 1000 / taktPerSecond
		bw.taktPerSecond = taktPerSecond
	} else {
		bw.taktPerSecond = DEFAULT_TAKT_PER_SECOND
		bw.taktTime = 1000 / DEFAULT_TAKT_PER_SECOND
	}

	// recalculate rate per takt time
	bw.SetRateLimit(bw.rateLimit)
}

// Set new bandwidth limit. rateLimit is bytes per second.
func (bw *Bwlimit) SetRateLimit(rateLimit int) {
	bw.streamMutex.Lock()
	defer bw.streamMutex.Unlock()

	bw.rateLimit = rateLimit
	bw.ratePerTaktTime = rateLimit / bw.taktPerSecond
	if bw.ratePerTaktTime < 1 {
		bw.ratePerTaktTime = 1
	}
}

// Wait for all stream close.
func (bw *Bwlimit) Wait() {
	bw.streamMutex.Lock()
	defer bw.streamMutex.Unlock()

	bw.activeIO.Wait()
}

// Issue new streamId.
func (bw *Bwlimit) issue() uint64 {
	bw.streamMutex.Lock()
	defer bw.streamMutex.Unlock()

	bw.activeIO.Add(1)
	bw.streamSeq++

	seq := bw.streamSeq
	bw.streamWindow[seq] = 0

	if !bw.manualTakt && !bw.timerRunning {
		bw.timerShutdown.Wait()
		go bw.taktTimer()
		bw.timerRunning = true
	}

	return seq
}

// Returns Reader with bandwidth limit.
func (bw *Bwlimit) Reader(r io.Reader) *Reader {
	q := &Reader{
		source:   r,
		streamId: bw.issue(),
		limiter:  bw,
	}
	return q
}

// Returns Writer with bandwidth limit.
func (bw *Bwlimit) Writer(w io.Writer) *Writer {
	q := &Writer{
		source:   w,
		streamId: bw.issue(),
		limiter:  bw,
	}
	return q
}

// Takt timer goroutine.
func (bw *Bwlimit) taktTimer() {
	for {
		bw.takt()
		if !bw.taktTimerShouldContinue() {
			break
		}
		time.Sleep(time.Duration(bw.taktTime) * time.Millisecond)
	}
	bw.timerShutdown.Done()
}

// Determine takt timer continue or not.
func (bw *Bwlimit) taktTimerShouldContinue() bool {
	bw.streamMutex.Lock()
	defer bw.streamMutex.Unlock()

	c := len(bw.streamWindow)
	if c < 1 {
		bw.timerRunning = false
		bw.timerShutdown.Add(1)
		return false
	} else {
		return true
	}
}

// Takt time. Increase window size for each stream.
func (bw *Bwlimit) takt() {
	bw.streamMutex.Lock()
	defer bw.streamMutex.Unlock()

	streams := len(bw.streamWindow)
	if streams < 1 {
		// skip
		return
	}

	chunkPerStream := bw.ratePerTaktTime / streams

	// at least 1 byte per stream per takt time
	if chunkPerStream < 1 {
		chunkPerStream = 1
	}

	for r, w := range bw.streamWindow {
		if w < bw.ratePerTaktTime {
			w += int(chunkPerStream)
			bw.streamWindow[r] = w
		}
	}
}

// Returns allowed windows size for streamId.
func (bw *Bwlimit) currentWindow(streamId uint64) int {
	if bw.rateLimit == RATE_UNLIMITED {
		return math.MaxInt32
	}

	bw.streamMutex.Lock()
	defer bw.streamMutex.Unlock()

	w, e := bw.streamWindow[streamId]
	if e {
		return w
	}
	return 0
}

// Consume bandwidth window for streamId.
func (bw *Bwlimit) consumeWindow(streamId uint64, consumeBytes int) {
	if bw.rateLimit == RATE_UNLIMITED {
		return
	}

	bw.streamMutex.Lock()
	defer bw.streamMutex.Unlock()

	w, e := bw.streamWindow[streamId]
	if !e {
		// ignore
		return
	}

	remainder := w - consumeBytes
	if remainder > 0 {
		bw.streamWindow[streamId] = remainder
	} else {
		bw.streamWindow[streamId] = 0
	}
}

// Close stream.
func (bw *Bwlimit) closeStream(streamId uint64) {
	bw.streamMutex.Lock()
	defer bw.streamMutex.Unlock()

	_, e := bw.streamWindow[streamId]
	if e {
		delete(bw.streamWindow, streamId)
		bw.activeIO.Done()
	}
}

// Reader with bandwidth limit.
type Reader struct {
	source   io.Reader
	streamId uint64
	limiter  *Bwlimit
	m        sync.Mutex
}

// Read returns (0, nil) when hit bandwidth limit.
func (r *Reader) Read(p []byte) (n int, err error) {
	r.m.Lock()
	defer r.m.Unlock()

	wnd := r.limiter.currentWindow(r.streamId)
	if wnd > 0 {
		n = len(p)
		if n > wnd {
			n = wnd
		}
		p = p[:n]
		n, err = r.source.Read(p)
		r.limiter.consumeWindow(r.streamId, n)
		if err == io.EOF {
			r.Close()
		}
	} else {
		if r.limiter.blocking {
			time.Sleep(time.Duration(r.limiter.taktTime) * time.Millisecond)
		}
	}
	return
}

func (r *Reader) Close() error {
	r.limiter.closeStream(r.streamId)
	return nil
}

// Writer with bandwidth limit.
type Writer struct {
	source   io.Writer
	streamId uint64
	limiter  *Bwlimit
	m        sync.Mutex
}

// Write returns (0, nil) when hit bandwidth limit.
func (w *Writer) Write(p []byte) (n int, err error) {
	w.m.Lock()
	defer w.m.Unlock()

	wnd := w.limiter.currentWindow(w.streamId)
	if wnd > 0 {
		n = len(p)
		if n > wnd {
			n = wnd
		}
		p = p[:n]
		n, err = w.source.Write(p)
		w.limiter.consumeWindow(w.streamId, n)
	} else {
		if w.limiter.blocking {
			time.Sleep(time.Duration(w.limiter.taktTime) * time.Millisecond)
		}
	}
	return
}

func (w *Writer) Close() error {
	w.limiter.closeStream(w.streamId)
	return nil
}
