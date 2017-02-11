package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

type Empty struct{}

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

func readInitial(in *bufio.Reader, ngdb *NgramDB) *NgramDB {
	for {
		line, _ := in.ReadString('\n')
		line = line[:len(line)-1]
		if "S" == line {
			fmt.Println("R")
			return ngdb
		}

		ngdb.AddNgram(line)
	}

	return nil // should never come here!!!
}

func printResults(results []NgramResult) {
	filtered := results
	if len(filtered) <= 0 {
		fmt.Println("-1")
		return
	}

	visited := make(map[string]Empty)

	//sort.Sort(ByAppearance(filtered))
	visited[filtered[0].Term] = Empty{}
	fmt.Print(filtered[0].Term)
	for _, r := range filtered[1:] {
		if _, ok := visited[r.Term]; !ok {
			visited[r.Term] = Empty{}
			fmt.Print("|" + r.Term)
		}
	}
	fmt.Println()
}

func partialQueryDoc(ngdb *NgramDB, doc string, startIdxEnd int) []NgramResult {
	localResults := make([]NgramResult, 0, 16)

	sz := len(doc)

	//fmt.Fprintln(os.Stderr, "partial query doc", sz, startIdxEnd)

	start := 0
	end := 0
	for start < startIdxEnd {
		// find start of word
		for start = end; start < startIdxEnd && doc[start] == ' '; start += 1 {
		}

		if start >= startIdxEnd {
			break
		}

		localResults = append(localResults, ngdb.FindNgrams(doc[start:], start)...)

		// find end of word
		for end = start; end < sz && doc[end] != ' '; end += 1 {
		}
	}

	return localResults
}

func queryDoc(ngdb *NgramDB, doc string) {
	//fmt.Fprintln(os.Stderr, "queryDoc", len(ngrams))
	docSz := len(doc)

	// single thread
	//results := partialQueryDoc(ngdb, doc, docSz, docSz)

	cores := runtime.NumCPU()
	chans := make([]chan []NgramResult, 0, cores+1)

	maxLenNgram := 100

	start := 0
	batchSz := int(docSz / cores)
	for start < docSz {
		startIdxEnd := int(math.Min(float64(start+batchSz), float64(docSz)))
		// make sure we are at the end of a word
		for ; startIdxEnd < docSz && doc[startIdxEnd] != ' '; startIdxEnd += 1 {
		}

		docEnd := int(math.Min(float64(startIdxEnd+maxLenNgram), float64(docSz)))

		//fmt.Fprintln(os.Stderr, docSz, cores, batchSz, start, startIdxEnd, docEnd)

		chRes := make(chan []NgramResult)
		chans = append(chans, chRes)

		go func(ngdb *NgramDB, partialDoc string, startIdxEnd int, chOut chan []NgramResult) {
			chRes <- partialQueryDoc(ngdb, partialDoc, startIdxEnd)
			close(chRes)
		}(ngdb, doc[start:docEnd], startIdxEnd-start, chRes)

		start = startIdxEnd
	}

	results := make([]NgramResult, 0, 128)
	for _, ch := range chans {
		partialResults := <-ch
		results = append(results, partialResults...)
	}

	printResults(results)
}

func processWorkload(in *bufio.Reader, ngdb *NgramDB) *NgramDB {
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

		if line[0] == 'D' {
			ngdb.RemoveNgram(line[2:])
		} else if line[0] == 'A' {
			ngdb.AddNgram(line[2:])
		} else if line[0] == 'Q' {
			queryDoc(ngdb, line[2:])
		} else if line[0] == 'F' {
			// TODO process the batch
			continue
		}
	}
	return ngdb
}

func main() {

	fmt.Fprintf(os.Stderr, "cores: [%d]\n", runtime.NumCPU())
	runtime.GOMAXPROCS(runtime.NumCPU())

	ngramDb := NewNgramDB()
	ngdb := &ngramDb

	in := bufio.NewReader(os.Stdin)

	ngdb = readInitial(in, ngdb)
	fmt.Fprintf(os.Stderr, "initial ngrams 1st level [%d]\n", len(ngdb.Trie.Root.Children))

	ngdb = processWorkload(in, ngdb)
	fmt.Fprintf(os.Stderr, "after workload ngrams 1st level [%d]\n", len(ngdb.Trie.Root.Children))
}
