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

type WorkerJob struct {
	Ngdb        *NgramDB
	Doc         string
	StartIdxEnd int
	ResultCh    chan []NgramResult
}

type WorkerStatus struct {
	Status string
}

type WorkerPool struct {
	Workers        int
	WorkerJobCh    []chan WorkerJob
	WorkerStatusCh []chan WorkerStatus
}

func NewWorkerPool() *WorkerPool {
	numWorkers := runtime.NumCPU()

	pool := &WorkerPool{
		Workers:        numWorkers,
		WorkerJobCh:    make([]chan WorkerJob, 0, numWorkers),
		WorkerStatusCh: make([]chan WorkerStatus, 0, numWorkers),
	}
	for i := 0; i < numWorkers; i++ {
		pool.WorkerJobCh = append(pool.WorkerJobCh, make(chan WorkerJob, 1))
		pool.WorkerStatusCh = append(pool.WorkerStatusCh, make(chan WorkerStatus, 1))

		go worker(pool.WorkerStatusCh[i], pool.WorkerJobCh[i])
	}

	return pool
}

func worker(chStatus chan WorkerStatus, chJobIn chan WorkerJob) {
	for {
		select {
		case job := <-chJobIn:
			job.ResultCh <- partialQueryDoc(job.Ngdb, job.Doc, job.StartIdxEnd)
			break
		case msg := <-chStatus:
			if msg.Status == "terminate" {
				return
			}
		}
	}
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

func queryDoc(ngdb *NgramDB, wpool *WorkerPool, doc string) {
	//fmt.Fprintln(os.Stderr, "queryDoc", len(ngrams))
	docSz := len(doc)

	cores := wpool.Workers
	chans := make([]chan []NgramResult, 0, cores)

	currentWorker := 0

	maxLenNgram := 100

	start := 0
	batchSz := int(docSz/cores) + 1
	for start < docSz {
		startIdxEnd := int(math.Min(float64(start+batchSz), float64(docSz)))
		// make sure we are at the end of a word
		for ; startIdxEnd < docSz && doc[startIdxEnd] != ' '; startIdxEnd += 1 {
		}

		docEnd := int(math.Min(float64(startIdxEnd+maxLenNgram), float64(docSz)))

		//fmt.Fprintln(os.Stderr, docSz, cores, batchSz, start, startIdxEnd, docEnd)

		chRes := make(chan []NgramResult)
		chans = append(chans, chRes)

		wpool.WorkerJobCh[currentWorker] <- WorkerJob{
			Ngdb:        ngdb,
			Doc:         doc[start:docEnd],
			StartIdxEnd: startIdxEnd - start,
			ResultCh:    chRes,
		}

		start = startIdxEnd
		currentWorker += 1
	}

	results := make([]NgramResult, 0, 128)
	for _, ch := range chans {
		results = append(results, (<-ch)...)
	}

	printResults(results)
}

func processWorkload(in *bufio.Reader, ngdb *NgramDB, workerPool *WorkerPool) *NgramDB {
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
			queryDoc(ngdb, workerPool, line[2:])
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

	workerPool := NewWorkerPool()

	in := bufio.NewReader(os.Stdin)

	ngdb = readInitial(in, ngdb)
	fmt.Fprintf(os.Stderr, "initial ngrams 1st level [%d]\n", len(ngdb.Trie.Root.Children))

	ngdb = processWorkload(in, ngdb, workerPool)
	fmt.Fprintf(os.Stderr, "after workload ngrams 1st level [%d]\n", len(ngdb.Trie.Root.Children))
}
