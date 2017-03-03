package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	//"strconv"
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

type ParallelQueryJobBatch struct {
	Jobs          []ParallelQueryJob
	ChResultBatch chan bytes.Buffer
}

type ParallelQueryJob struct {
	OpQ OpQuery
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

type WorkerMasterJob struct {
	Ngdb          *NgramDB
	Wpool         *WorkerPool
	WpoolStartIdx int
	ChJobIn       chan ParallelQueryJobBatch
}

type WorkerPool struct {
	Workers      int
	WorkersTotal int
	ParallelQ    int

	MaxDocSplit int

	WorkerJobCh       []chan WorkerJob
	WorkerMasterJobCh []chan WorkerMasterJob
	WorkerStatusCh    []chan WorkerStatus
}

func NewWorkerPool() *WorkerPool {
	parallelq := 1
	numWorkers := 0 // 2+ means allow inner splitting of docs

	pool := &WorkerPool{
		MaxDocSplit: 1 << 30,
		Workers:     numWorkers,
		ParallelQ:   parallelq,
	}
	return pool
}

// @worker - partial document processing
func workerInnerJobRun(job *WorkerJob) {
	job.ResultCh <- partialQueryDoc(job.Ngdb, job.Doc, job.StartIdxEnd, job.OpIdx)
}
func partialQueryDoc(ngdb *NgramDB, doc string, startIdxEnd int, opIdx int) []NgramResult {
	localResults := make([]NgramResult, 0, 32)

	sz := len(doc)

	start := 0
	end := 0
	for start < startIdxEnd {
		// find start of word
		for start = end; start < startIdxEnd && doc[start] == ' '; start += 1 {
		}

		if start >= startIdxEnd {
			break
		}

		tresult := ngdb.FindNgrams(doc[start:], start, opIdx)
		if len(tresult) > 0 {
			localResults = append(localResults, tresult...)
		}

		// find end of word
		for end = start; end < sz && doc[end] != ' '; end += 1 {
		}
	}

	return localResults
}

// @master worker - per doc
func queryDispatcher(ngdb *NgramDB, wpool *WorkerPool, opQ OpQuery) bytes.Buffer {
	//defer timeStop(time.Now(), "qd()")

	workers := 1
	doc := opQ.Doc
	opIdx := opQ.OpIdx
	docSz := len(doc)

	//fmt.Fprintln(os.Stderr, "qsz", docSz)

	if docSz == 0 {
		return outputBytes(nil)
	}

	if wpool.Workers > 1 {
		if docSz%wpool.MaxDocSplit > 0 {
			workers = docSz/wpool.MaxDocSplit + 1
		} else {
			workers = docSz / wpool.MaxDocSplit
		}
	}

	if workers == 1 {
		return outputBytes(filterResults(partialQueryDoc(ngdb, doc, docSz, opIdx)))
	}

	// TODO Estimate this better based on the NGDB
	//maxLenNgram := docSz

	chans := make([]chan []NgramResult, 0, workers)

	// The DIV might less than cores so add 1 to cover all the doc
	batchSz := int(docSz/workers) + 1
	start := 0
	for widx := 0; widx < workers; widx++ {
		startIdxEnd := int(math.Min(float64(start+batchSz), float64(docSz)))
		// make sure we are at the end of a word
		for ; startIdxEnd < docSz && doc[startIdxEnd] != ' '; startIdxEnd++ {
		}

		//docEnd := int(math.Min(float64(startIdxEnd+maxLenNgram), float64(docSz)))
		docEnd := docSz

		chRes := make(chan []NgramResult, 1)
		chans = append(chans, chRes)

		go workerInnerJobRun(&WorkerJob{
			Ngdb:        ngdb,
			Doc:         doc[start:docEnd],
			StartIdxEnd: startIdxEnd - start,
			OpIdx:       opIdx,
			ResultCh:    chRes,
		})

		start = startIdxEnd
	}

	// Gather, filter and return results
	results := make([]NgramResult, 0, 128)
	for _, ch := range chans {
		results = append(results, (<-ch)...)
	}
	return outputBytes(filterResults(results))
}

// @master worker
func parallelQueryWorkerRoundBatch(ngdb *NgramDB, wpool *WorkerPool, jobs ParallelQueryJobBatch) {
	var bResult bytes.Buffer
	for _, job := range jobs.Jobs {
		lb := queryDispatcher(ngdb, wpool, job.OpQ)
		bResult.Write(lb.Bytes())
	}

	jobs.ChResultBatch <- bResult
}

// @main
func queryBatchDispatcherRoundBatch(ngdb *NgramDB, wpool *WorkerPool, opQ []OpQuery) {
	//fmt.Fprintln(os.Stderr, "qdb():"+strconv.Itoa(len(opQ)))
	//defer timeStop(time.Now(), "qdb():"+strconv.Itoa(len(opQ)))
	defer timeSave(time.Now(), "qdb()")

	qsz := len(opQ)
	if qsz == 0 {
		return
	}

	activePQ := wpool.ParallelQ
	if wpool.ParallelQ > qsz {
		activePQ = qsz
	}

	rounds := qsz / activePQ
	extraRounds := qsz % activePQ

	var startIdx, endIdx int
	chansResultsBatch := make([]chan bytes.Buffer, 0, activePQ)
	for i := 0; i < activePQ; i++ {
		chansResultsBatch = append(chansResultsBatch, make(chan bytes.Buffer, 1))

		// Create the job batch for this parallel worker
		batchedJobs := make([]ParallelQueryJob, 0, rounds)

		// Uniformly divide tasks
		if i < extraRounds {
			startIdx = rounds*i + i
			endIdx = startIdx + rounds + 1
		} else {
			startIdx = rounds*i + extraRounds
			endIdx = startIdx + rounds
		}
		if endIdx > qsz {
			endIdx = qsz
		}
		for qidx := startIdx; qidx < endIdx; qidx++ {
			batchedJobs = append(batchedJobs, ParallelQueryJob{opQ[qidx]})
		}

		batchJob := ParallelQueryJobBatch{batchedJobs, chansResultsBatch[i]}
		//fmt.Fprintln(os.Stderr, len(batchedJobs))

		// Start the worker
		go parallelQueryWorkerRoundBatch(ngdb, wpool, batchJob)
	}

	// Gather and print results
	for pidx := 0; pidx < activePQ; pidx++ {
		printResults(<-chansResultsBatch[pidx])
	}
}

func processWorkload(in *bufio.Reader, ngdb *NgramDB, workerPool *WorkerPool, opIdx int) *NgramDB {
	defer timeStop(time.Now(), "process()")

	opQs := make([]OpQuery, 0, 256)

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
		} else if line[0] == 'F' {
			queryBatchDispatcherRoundBatch(ngdb, workerPool, opQs)
			//queryBatchDispatcherSingleQueue(ngdb, workerPool, opQs)
			opQs = opQs[:0]
		}
	}

	return ngdb
}

func main() {
	defer timeFinish(time.Now(), "main()")
	runtime.GOMAXPROCS(runtime.NumCPU())

	ngramDb := NewNgramDB()
	ngdb := &ngramDb

	workerPool := NewWorkerPool()
	fmt.Fprintf(os.Stderr, "cores[%d] parallelq[%d] workers[%d] maxdocsplit[%d]\n", runtime.NumCPU(), workerPool.ParallelQ, workerPool.Workers, workerPool.MaxDocSplit)

	//in := bufio.NewReaderSize(os.Stdin, 1<<20)
	in := bufio.NewReader(os.Stdin)

	ngdb, opIdx := readInitial(in, ngdb)

	ngdb = processWorkload(in, ngdb, workerPool, opIdx)

	fmt.Fprintln(os.Stderr, "trie sz", ChildrenTrie, len(ngdb.Trie.Root.ChildrenIndex))
}

/////////////////// HELPERS /////////////////////

var timers map[string]time.Duration = make(map[string]time.Duration)

func timeSave(start time.Time, msg string) {
	timers[msg] += time.Since(start)
}
func timeFinish(start time.Time, msg string) {
	total := time.Since(start)
	fmt.Fprintf(os.Stderr, "%s[%s]\n", msg, total)

	for k, v := range timers {
		fmt.Fprintf(os.Stderr, "%s[%s]\n", k, v)
	}
}

func timeStop(start time.Time, msg string) {
	total := time.Since(start)
	fmt.Fprintf(os.Stderr, "%s[%s]\n", msg, total)
}

func readInitial(in *bufio.Reader, ngdb *NgramDB) (*NgramDB, int) {
	defer timeSave(time.Now(), "r()")

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

func printResults(result bytes.Buffer) {
	result.WriteTo(os.Stdout)
}

func filterResults(l []NgramResult) []NgramResult {
	if len(l) == 0 {
		return l
	}

	visited := make(map[string]Empty, len(l))
	filtered := l[:0]
	for _, r := range l {
		if _, ok := visited[r.Term]; !ok {
			visited[r.Term] = Empty{}
			filtered = append(filtered, r)
		}
	}

	return filtered
}

func outputBytes(results []NgramResult) bytes.Buffer {
	var b bytes.Buffer

	if len(results) <= 0 {
		b.Write([]byte("-1\n"))
		return b
	}

	sepBytes := []byte("|")

	b.Write([]byte(results[0].Term))
	for _, r := range results[1:] {
		b.Write(sepBytes)
		b.Write([]byte(r.Term))
	}
	b.Write([]byte("\n"))

	return b
}
