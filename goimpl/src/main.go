package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"time"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

type Empty struct{}

type OpQuery struct {
	Doc   string
	OpIdx int
}

type WorkerJob struct {
	Ngdb        *NgramDB
	Doc         string
	StartIdxEnd int
	OpIdx       int
	ResultCh    chan []NgramResult
}

type WorkerStatus struct {
	Status string
}

type WorkerPool struct {
	Workers         int
	WorkersInternal int
	WorkerJobCh     []chan WorkerJob
	WorkerStatusCh  []chan WorkerStatus
}

func NewWorkerPool() *WorkerPool {
	numWorkers := runtime.NumCPU()

	pool := &WorkerPool{
		Workers:         numWorkers,
		WorkersInternal: 0,
		WorkerJobCh:     make([]chan WorkerJob, 0, numWorkers),
		WorkerStatusCh:  make([]chan WorkerStatus, 0, numWorkers),
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
			job.ResultCh <- partialQueryDoc(job.Ngdb, job.Doc, job.StartIdxEnd, job.OpIdx)
			break
		case msg := <-chStatus:
			if msg.Status == "terminate" {
				return
			}
		}
	}
}

func partialQueryDoc(ngdb *NgramDB, doc string, startIdxEnd int, opIdx int) []NgramResult {
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

		localResults = append(localResults, ngdb.FindNgrams(doc[start:], start, opIdx)...)

		// find end of word
		for end = start; end < sz && doc[end] != ' '; end += 1 {
		}
	}

	return localResults
}

func queryDispatcher(ngdb *NgramDB, wpool *WorkerPool, doc string, opIdx int) {
	defer timeSave(time.Now(), "qd()")

	//fmt.Fprintln(os.Stderr, "queryDoc", len(ngrams))
	docSz := len(doc)

	cores := wpool.Workers
	chans := make([]chan []NgramResult, 0, cores)

	currentWorker := 0

	maxLenNgram := 100

	start := 0

	// The DIV might less than cores so add 1 to cover all the doc
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
			OpIdx:       opIdx,
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

func queryBatchDispatcher(ngdb *NgramDB, wpool *WorkerPool, opQ []OpQuery) {
	defer timeStop(time.Now(), "qdb():"+strconv.Itoa(len(opQ)))
	for _, q := range opQ {
		queryDispatcher(ngdb, wpool, q.Doc, q.OpIdx)
	}
}

func processWorkload(in *bufio.Reader, ngdb *NgramDB, workerPool *WorkerPool, opIdx int) *NgramDB {
	defer timeStop(time.Now(), "process()")

	opQs := make([]OpQuery, 0, 64)

	// For cleanup purposes
	//lowOpIdx := opIdx
	for {
		line, err := in.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		line = line[:len(line)-1]
		opIdx += 1

		if line[0] == 'D' {
			ngdb.RemoveNgram(line[2:], opIdx)
		} else if line[0] == 'A' {
			ngdb.AddNgram(line[2:], opIdx)
		} else if line[0] == 'Q' {
			opQs = append(opQs, OpQuery{line[2:], opIdx})
			//queryDispatcher(ngdb, workerPool, line[2:], opIdx)
		} else if line[0] == 'F' {
			queryBatchDispatcher(ngdb, workerPool, opQs)
			opQs = opQs[:0]

			//lowOpIdx = opIdx
			continue
		}
	}
	return ngdb
}

func main() {
	defer timeFinish(time.Now(), "main()")

	fmt.Fprintf(os.Stderr, "cores: [%d]\n", runtime.NumCPU())
	runtime.GOMAXPROCS(runtime.NumCPU())

	ngramDb := NewNgramDB()
	ngdb := &ngramDb

	workerPool := NewWorkerPool()

	in := bufio.NewReader(os.Stdin)

	ngdb, opIdx := readInitial(in, ngdb)
	fmt.Fprintf(os.Stderr, "initial ngrams 1st level [%d]\n", len(ngdb.Trie.Root.Children))

	ngdb = processWorkload(in, ngdb, workerPool, opIdx)
	fmt.Fprintf(os.Stderr, "after workload ngrams 1st level [%d]\n", len(ngdb.Trie.Root.Children))
}

/////////////////// HELPERS /////////////////////

var timers map[string]time.Duration = make(map[string]time.Duration)

func timeSave(start time.Time, msg string) {
	timers[msg] += time.Since(start)
}
func timeFinish(start time.Time, msg string) {
	total := time.Since(start)
	fmt.Fprintf(os.Stderr, "%s - Time[%s]\n", msg, total)

	for k, v := range timers {
		fmt.Fprintf(os.Stderr, "%s - Time[%s]\n", k, v)
	}
}

func timeStop(start time.Time, msg string) {
	total := time.Since(start)
	fmt.Fprintf(os.Stderr, "%s - Time[%s]\n", msg, total)
}

func readInitial(in *bufio.Reader, ngdb *NgramDB) (*NgramDB, int) {
	defer timeStop(time.Now(), "readInitial")

	opIdx := 0
	for {
		line, _ := in.ReadString('\n')
		line = line[:len(line)-1]
		if "S" == line {
			fmt.Println("R")
			return ngdb, opIdx
		}

		opIdx += 1

		ngdb.AddNgram(line, opIdx)
	}

	return nil, 0 // should never come here!!!
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
