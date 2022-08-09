package main

import (
	"bufio"
	"io"
	"os"
	"path/filepath"

	"github.com/zeebo/xxh3"
)

var (
	newline = []byte("\n")
	none    = struct{}{}
)

func main() {
	err := RunE(os.Args)
	if err != nil {
		_, _ = os.Stderr.WriteString(err.Error())
		_, _ = os.Stderr.Write(newline)
		os.Exit(1)
	}
}

func RunE(args []string) error {
	lines := map[uint64]struct{}{}
	if len(args) == 1 {
		if err := ProcessSource(lines, os.Stdin, os.Stdout); err != nil {
			return err
		}
	}

	for _, a := range args[1:] {
		m, err := filepath.Glob(a)
		if err != nil {
			return err
		}
		for _, fn := range m {
			f, err := os.Open(fn)
			if err != nil {
				return err
			}
			err = ProcessSource(lines, f, os.Stdout)
			f.Close()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func ProcessSource(lines map[uint64]struct{}, in io.Reader, out io.Writer) error {
	scn := bufio.NewScanner(in)
	for scn.Scan() {
		t := scn.Bytes()
		if len(t) == 0 {
			continue
		}
		// give up at 25k lines
		if len(lines) < 25000 {
			h := xxh3.Hash(t)
			if _, ok := lines[h]; ok {
				continue
			}
			lines[h] = none
		}
		if _, err := out.Write(t); err != nil {
			return err
		}
		if _, err := out.Write(newline); err != nil {
			return err
		}
	}
	return scn.Err()
}
