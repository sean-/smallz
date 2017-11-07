package bwlimit

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"testing"
	"time"
)

func TestBwlimit_Unlimited(t *testing.T) {
	bw := NewBwlimit(RATE_UNLIMITED, true)
	seq := make([]byte, 1000)
	r := bytes.NewReader(seq)
	wr := bw.Reader(r)
	buf := make([]byte, 1000)

	n, err := wr.Read(buf)
	if n != 1000 || err != nil {
		t.Errorf("Invalid rate n[%d] or err[%s]", n, err)
	}
}

func TestBwlimit_SetBlocking(t *testing.T) {
	bw := NewBwlimit(1000, true)
	bw.SetTaktPerSecond(10)
	seq := make([]byte, 1000)
	r := bytes.NewReader(seq)
	wr := bw.Reader(r)

	count := 0

	buf := make([]byte, 1000)
	for {
		_, err := wr.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
	}

	if count > 100 {
		t.Errorf("Too many loop: %d", count)
	}
}

func TestBwlimit_Reader(t *testing.T) {
	bw := NewBwlimit(1000, false)

	// generate content
	inSeq := make([]byte, 1000)
	n, err := rand.Read(inSeq)
	if err != nil || n != 1000 {
		t.Fail()
	}
	r := bytes.NewReader(inSeq)
	wr := bw.Reader(r)

	outSeq := make([]byte, 1000)
	offset := 0
	for {
		buf := make([]byte, 1000)
		n, err = wr.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if n > 0 {

		}
		for i := 0; i < n; i++ {
			outSeq[i+offset] = buf[i]
		}
		offset += n
	}

	for i := 0; i < 1000; i++ {
		if inSeq[i] != outSeq[i] {
			t.Error("Invalid content found")
		}
	}

	// wait for all reader close
	bw.Wait()
}

func TestBwlimit_Writer(t *testing.T) {
	bw := NewBwlimit(1000, false)
	bw.SetTaktPerSecond(10)
	seq := make([]byte, 8000)
	w := bytes.NewBuffer(seq)
	fw := bw.Writer(w)
	buf := make([]byte, 1000)
	wrote := 0

	for i := 0; i < 10; i++ {
		n, err := fw.Write(buf)
		if err != nil {
			t.Fatal(err)
		}
		wrote += n
		time.Sleep(100 * time.Millisecond)
	}
	if wrote > 1000 {
		t.Errorf("Transfer too fast: %d bytes per second", wrote)
	}

}
func TestBwlimit_BandwidthSingle(t *testing.T) {
	expectedTransferSeconds := 10
	rate := 1000
	bw := NewBwlimit(rate, false)
	bw.SetTaktPerSecond(10)
	seq := make([]byte, rate*expectedTransferSeconds)
	f := bytes.NewReader(seq)

	start := time.Now()
	timeout := start.Add(time.Duration(2) * time.Duration(expectedTransferSeconds) * time.Second)

	fr := bw.Reader(f)
	buf := make([]byte, rate)
	for {
		_, err := fr.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if start.After(timeout) {
			t.Fatal(errors.New("Timeout"))
		}
	}

	threshold := start.Add(time.Duration(expectedTransferSeconds)*time.Second - 100*time.Millisecond)
	if time.Now().Before(threshold) {
		t.Errorf("Transfer too fast: now(%v) threashold(%v)", time.Now(), threshold)
	}
}

func TestBwlimit_BandwidthDouble(t *testing.T) {
	expectedTransferSeconds := 10
	rate := 1000
	bw := NewBwlimit(rate, false)
	bw.SetTaktPerSecond(10)
	seq1 := make([]byte, rate*expectedTransferSeconds/2)
	seq2 := make([]byte, rate*expectedTransferSeconds/2)
	f1 := bytes.NewReader(seq1)
	f2 := bytes.NewReader(seq2)

	start := time.Now()
	timeout := start.Add(time.Duration(2) * time.Duration(expectedTransferSeconds) * time.Second)

	fr1 := bw.Reader(f1)
	fr2 := bw.Reader(f2)
	buf := make([]byte, rate)
	for {
		_, err := fr1.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		_, err = fr2.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if start.After(timeout) {
			t.Fatal(errors.New("Timeout"))
		}
	}

	threshold := start.Add(time.Duration(expectedTransferSeconds)*time.Second - 100*time.Millisecond)
	if time.Now().Before(threshold) {
		t.Errorf("Transfer too fast: now(%v) threashold(%v)", time.Now(), threshold)
	}
}

func TestBwlimit_ManualTakt(t *testing.T) {
	bw := NewBwlimit(1000, false)
	bw.SetTaktPerSecond(10)
	bw.manualTakt = true

	if bw.rateLimit != 1000 {
		t.Error("Invalid state")
	}
	if bw.taktTime != 100 {
		t.Error("Invalid state")
	}
	if bw.taktPerSecond != 10 {
		t.Error("Invalid state")
	}
	if bw.ratePerTaktTime != 100 {
		t.Error("Invalid state")
	}

	r1seq := make([]byte, 110)
	r1 := bytes.NewReader(r1seq)
	r1bw := bw.Reader(r1)
	r1buf := make([]byte, 1000)

	r2seq := make([]byte, 210)
	r2 := bytes.NewReader(r2seq)
	r2bw := bw.Reader(r2)
	r2buf := make([]byte, 1000)

	r1r, _ := r1bw.Read(r1buf)
	r2r, _ := r2bw.Read(r2buf)

	if r1r != 0 || r2r != 0 {
		t.Errorf("Read before first takt time r1[%d], r2[%d]", r1r, r2r)
	}
	bw.takt()

	r1r, _ = r1bw.Read(r1buf)
	r2r, _ = r2bw.Read(r2buf)

	if r1r != 50 || r2r != 50 {
		t.Errorf("Unexpected traffic r1[%d], r2[%d]", r1r, r2r)
	}

	bw.takt()

	r1r, _ = r1bw.Read(r1buf)
	r2r, _ = r2bw.Read(r2buf)

	if r1r != 50 || r2r != 50 {
		t.Errorf("Unexpected traffic r1[%d], r2[%d]", r1r, r2r)
	}

	bw.takt()

	r1r, _ = r1bw.Read(r1buf)
	r2r, _ = r2bw.Read(r2buf)

	if r1r != 10 || r2r != 50 {
		t.Errorf("Unexpected traffic r1[%d], r2[%d]", r1r, r2r)
	}

	bw.takt()

	r1r, _ = r1bw.Read(r1buf)
	r2r, _ = r2bw.Read(r2buf)

	if r1r != 0 || r2r != 50 {
		t.Errorf("Unexpected traffic r1[%d], r2[%d]", r1r, r2r)
	}

	r3seq := make([]byte, 70)
	r3 := bytes.NewReader(r3seq)
	r3bw := bw.Reader(r3)
	r3buf := make([]byte, 1000)

	bw.takt()

	r1r, _ = r1bw.Read(r1buf)
	r2r, _ = r2bw.Read(r2buf)
	r3r, _ := r3bw.Read(r3buf)

	if r1r != 0 || r2r != 10 || r3r != 50 {
		t.Errorf("Unexpected traffic r1[%d], r2[%d], r3[%d]", r1r, r2r, r3r)
	}

	bw.takt()

	r1r, _ = r1bw.Read(r1buf)
	r2r, _ = r2bw.Read(r2buf)
	r3r, _ = r3bw.Read(r3buf)

	if r1r != 0 || r2r != 0 || r3r != 20 {
		t.Errorf("Unexpected traffic r1[%d], r2[%d], r3[%d]", r1r, r2r, r3r)
	}

	bw.takt()

	r1r, _ = r1bw.Read(r1buf)
	r2r, _ = r2bw.Read(r2buf)
	r3r, _ = r3bw.Read(r3buf)

	if r1r != 0 || r2r != 0 || r3r != 0 {
		t.Errorf("Unexpected traffic r1[%d], r2[%d], r3[%d]", r1r, r2r, r3r)
	}
}
