package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

type Ngram struct {
	Value string
	Valid bool
}

type Result struct {
	NgramValue Ngram
	Pos        int
}

type ByAppearance []Result

func (a ByAppearance) Len() int      { return len(a) }
func (a ByAppearance) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByAppearance) Less(i, j int) bool {
	if a[i].Pos < a[j].Pos {
		return true
	} else if a[i].Pos > a[j].Pos {
		return false
	}
	return len(a[i].NgramValue.Value) < len(a[j].NgramValue.Value)
}

func deleteNgram(ngrams []Ngram, ngram string) []Ngram {
	for i, cur := range ngrams {
		if ngram == cur.Value {
			ngrams[i].Valid = false
			return ngrams
		}
	}
	return ngrams
}

func readInitial(in *bufio.Reader) []Ngram {
	ngrams := make([]Ngram, 0, 10000)

	for {
		line, _ := in.ReadString('\n')
		line = line[:len(line)-1]
		if "S" == line {
			fmt.Println("R")
			return ngrams
		}
		ngrams = append(ngrams, Ngram{line, true})
	}
	return nil
}

func filterInvalidResults(results []Result) []Result {
	res := results[:0]
	for _, cr := range results {
		if cr.NgramValue.Valid {
			res = append(res, cr)
		}
	}

	return res
}

func printResults(results []Result) {
	/*
		filtered := filterInvalidResults(results)
	*/
	filtered := results
	if len(filtered) <= 0 {
		fmt.Printf("-1\n")
		return
	}

	sort.Sort(ByAppearance(filtered))
	fmt.Print(filtered[0].NgramValue.Value)
	for _, r := range filtered[1:] {
		fmt.Print("|" + r.NgramValue.Value)
	}
	fmt.Println()
}

func queryDoc(ngrams []Ngram, doc string) {
	doc = " " + doc + " "
	results := make([]Result, 0, 100)

	//fmt.Fprintln(os.Stderr, "queryDoc", len(ngrams))

	for _, ngram := range ngrams {
		if !ngram.Valid {
			continue
		}
		ngramValue := " " + ngram.Value + " "
		pos := strings.Index(doc, ngramValue)
		if pos != -1 {
			results = append(results, Result{ngram, pos})
		}
	}

	printResults(results)
}

const (
	Dbyte = 'D'
	Abyte = 'A'
	Qbyte = 'Q'
	Fbyte = 'F'
)

func processWorkload(in *bufio.Reader, ngrams []Ngram) []Ngram {
	for {
		line, err := in.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		line = line[:len(line)-1]
		//fmt.Fprintln(os.Stderr, line)

		if line[0] == Dbyte {
			ngrams = deleteNgram(ngrams, line[2:])
		} else if line[0] == Abyte {
			ngrams = append(ngrams, Ngram{line[2:], true})
		} else if line[0] == Qbyte {
			queryDoc(ngrams, line[2:])
		} else if line[0] == Fbyte {
			// TODO process the batch
			continue
		}
	}
	return ngrams
}

func main() {

	fmt.Fprintf(os.Stderr, "cores: [%d]\n", runtime.NumCPU())
	runtime.GOMAXPROCS(runtime.NumCPU())

	in := bufio.NewReader(os.Stdin)

	ngrams := readInitial(in)
	fmt.Fprintf(os.Stderr, "initial ngrams [%d]\n", len(ngrams))

	ngrams = processWorkload(in, ngrams)
	fmt.Fprintf(os.Stderr, "after workload ngrams [%d]\n", len(ngrams))
}
