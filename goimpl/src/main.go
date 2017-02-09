package main

import (
	"bufio"
	"fmt"
	"io"
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

func printResults(results []Result) {
	filtered := results
	if len(filtered) <= 0 {
		fmt.Printf("-1\n")
		return
	}

	visited := make(map[string]Empty)

	//sort.Sort(ByAppearance(filtered))
	visited[filtered[0].NgramValue.Value] = Empty{}
	fmt.Print(filtered[0].NgramValue.Value)
	for _, r := range filtered[1:] {
		if _, ok := visited[r.NgramValue.Value]; !ok {
			visited[r.NgramValue.Value] = Empty{}
			fmt.Print("|" + r.NgramValue.Value)
		}
	}
	fmt.Println()
}

func partialQueryDoc(ngdb *NgramDB, doc string) []Result {
	results := make([]Result, 0, 32)

	localResults := make([]NgramResult, 0, 16)

	start := 0
	end := 0
	sz := len(doc)
	for start < sz {
		// find start of word
		for start = end; start < sz && doc[start] == ' '; start += 1 {
		}
		// find end of word
		for end = start; end < sz && doc[end] != ' '; end += 1 {
		}

		if start >= sz {
			break
		}

		localResults = append(localResults, ngdb.FindNgrams(doc[start:], start)...)
	}

	for _, lr := range localResults {
		results = append(results, Result{Ngram{lr.Word, true}, lr.Pos})
	}

	return results
}

func queryDoc(ngdb *NgramDB, doc string) {
	//fmt.Fprintln(os.Stderr, "queryDoc", len(ngrams))

	results := partialQueryDoc(ngdb, doc)
	/*
		cores := runtime.NumCPU()
		chans := make([]chan []Result, 0, cores+1)

		sz := len(ngrams)
		start := 0
		batchSz := int(sz / cores)
		//var wg sync.WaitGroup
		for start < sz {
			//wg.Add(1)

			end := int(math.Min(float64(start+batchSz), float64(sz)))

			chRes := make(chan []Result)
			chans = append(chans, chRes)

			go func(ngs []Ngram, partialDoc string, chOut chan []Result) {
				chRes <- partialQueryDoc(ngs, partialDoc)
				close(chRes)
			}(ngrams[start:end], doc, chRes)

			start = end
		}
		//wg.Wait()

		results := make([]Result, 0, 128)
		for _, ch := range chans {
			partialResults := <-ch
			results = append(results, partialResults...)
		}
	*/
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
