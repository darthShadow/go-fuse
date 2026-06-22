// Copyright 2016 the Go-FUSE Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package benchmark

// Routines for benchmarking fuse.

import (
	"bufio"
	"io"
	"log"
	"os"
)

func ReadLines(name string) []string {
	f, err := os.Open(name)
	if err != nil {
		log.Fatal("Open: ", err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var lines []string
	for {
		line, err := r.ReadString('\n')
		if len(line) > 0 {
			if line[len(line)-1] == '\n' {
				line = line[:len(line)-1]
			}
			if len(line) > 0 {
				lines = append(lines, line)
			}
		}
		if err == nil {
			continue
		}
		if err == io.EOF {
			break
		}
		log.Fatal("ReadString: ", err)
	}
	return lines
}
