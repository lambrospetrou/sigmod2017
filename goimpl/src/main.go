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
	ChResultBatch chan []bytes.Buffer
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
	Workers           int
	WorkersTotal      int
	ParallelQ         int
	WorkerJobCh       []chan WorkerJob
	WorkerMasterJobCh []chan WorkerMasterJob
	WorkerStatusCh    []chan WorkerStatus
}

func NewWorkerPool() *WorkerPool {
	totalWorkers := 1
	parallelq := 1
	numWorkers := 1

	pool := &WorkerPool{
		WorkersTotal:      totalWorkers,
		Workers:           numWorkers,
		ParallelQ:         parallelq,
		WorkerJobCh:       make([]chan WorkerJob, 0, totalWorkers),
		WorkerMasterJobCh: make([]chan WorkerMasterJob, 0, totalWorkers),
		WorkerStatusCh:    make([]chan WorkerStatus, 0, totalWorkers),
	}
	for i := 0; i < totalWorkers; i++ {
		pool.WorkerJobCh = append(pool.WorkerJobCh, make(chan WorkerJob, 1))
		pool.WorkerMasterJobCh = append(pool.WorkerMasterJobCh, make(chan WorkerMasterJob, 1))
		pool.WorkerStatusCh = append(pool.WorkerStatusCh, make(chan WorkerStatus, 1))

		go worker(pool.WorkerStatusCh[i], pool.WorkerMasterJobCh[i], pool.WorkerJobCh[i])
	}

	return pool
}

// @all threads/workers
func worker(chStatus chan WorkerStatus, chMasterJobIn chan WorkerMasterJob, chJobIn chan WorkerJob) {
	for {
		select {
		case job := <-chJobIn:
			workerInnerJobRun(job)
			break
		case job := <-chMasterJobIn:
			parallelQueryWorkerRoundBatch(job.Ngdb, job.Wpool, job.WpoolStartIdx, job.ChJobIn)
			break
		case msg := <-chStatus:
			if msg.Status == "terminate" {
				return
			}
		}
	}
}

// @worker - partial document processing
func workerInnerJobRun(job WorkerJob) {
	job.ResultCh <- partialQueryDoc(job.Ngdb, job.Doc, job.StartIdxEnd, job.OpIdx)
}
func partialQueryDoc(ngdb *NgramDB, doc string, startIdxEnd int, opIdx int) []NgramResult {
	localResults := make([]NgramResult, 0, 64)

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
func queryDispatcher(ngdb *NgramDB, wpool *WorkerPool, wpoolStartIdx int, opQ OpQuery) bytes.Buffer {
	//defer timeStop(time.Now(), "qd()")

	doc := opQ.Doc
	opIdx := opQ.OpIdx
	docSz := len(doc)

	if docSz == 0 {
		return outputBytes(nil)
	}

	workers := wpool.Workers
	chans := make([]chan []NgramResult, 0, workers)

	if workers == 1 {
		return outputBytes(filterResults(partialQueryDoc(ngdb, doc, docSz, opIdx)))
	}

	// TODO Estimate this better based on the NGDB
	maxLenNgram := docSz

	currentWorker := wpoolStartIdx

	// The DIV might less than cores so add 1 to cover all the doc
	batchSz := int(docSz/workers) + 1
	start := 0
	for start < docSz {
		startIdxEnd := int(math.Min(float64(start+batchSz), float64(docSz)))
		// make sure we are at the end of a word
		for ; startIdxEnd < docSz && doc[startIdxEnd] != ' '; startIdxEnd += 1 {
		}

		docEnd := int(math.Min(float64(startIdxEnd+maxLenNgram), float64(docSz)))

		chRes := make(chan []NgramResult, 1)
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

	// The master worker always does the last partial job too!
	// masterworker
	if currentWorker-wpoolStartIdx == workers {
		workerInnerJobRun(<-wpool.WorkerJobCh[currentWorker-1])
	}

	// Gather, filter and return results
	results := make([]NgramResult, 0, 16)
	for _, ch := range chans {
		results = append(results, (<-ch)...)
	}
	return outputBytes(filterResults(results))
}

// @master worker
func parallelQueryWorkerRoundBatch(ngdb *NgramDB, wpool *WorkerPool, wpoolStartIdx int, chJobIn chan ParallelQueryJobBatch) {
	results := make([]bytes.Buffer, 0, 64)

	jobs := <-chJobIn
	for _, job := range jobs.Jobs {
		results = append(results, queryDispatcher(ngdb, wpool, wpoolStartIdx, job.OpQ))
	}

	jobs.ChResultBatch <- results
}

// @main
func queryBatchDispatcherRoundBatch(ngdb *NgramDB, wpool *WorkerPool, opQ []OpQuery) {
	//defer timeStop(time.Now(), "qdb():"+strconv.Itoa(len(opQ)))
	defer timeSave(time.Now(), "qdb()")

	qsz := len(opQ)
	rounds := qsz / wpool.ParallelQ
	if qsz%wpool.ParallelQ > 0 {
		rounds += 1
	}

	chansResultsBatch := make([]chan []bytes.Buffer, 0, wpool.ParallelQ)
	chansJobs := make([]chan ParallelQueryJobBatch, 0, wpool.ParallelQ)
	for i := 0; i < wpool.ParallelQ; i++ {
		chansResultsBatch = append(chansResultsBatch, make(chan []bytes.Buffer, 1))
		chansJobs = append(chansJobs, make(chan ParallelQueryJobBatch, 1))

		// Create the job batch for this parallel worker
		batchedJobs := make([]ParallelQueryJob, 0, rounds)

		startIdx := rounds * i
		endIdx := startIdx + rounds
		if endIdx > qsz {
			endIdx = qsz
		}
		for qidx := startIdx; qidx < endIdx; qidx++ {
			batchedJobs = append(batchedJobs, ParallelQueryJob{opQ[qidx]})
		}

		//fmt.Fprintln(os.Stderr, len(batchedJobs[i]))
		chansJobs[i] <- ParallelQueryJobBatch{batchedJobs, chansResultsBatch[i]}

		// Start the worker
		//go parallelQueryWorkerRoundBatch(ngdb, wpool, i*wpool.Workers, chansJobs[i])

		// masterworker
		wpool.WorkerMasterJobCh[i*wpool.Workers+wpool.Workers-1] <- WorkerMasterJob{
			ngdb, wpool, i * wpool.Workers, chansJobs[i],
		}

	}

	// Gather and print results
	for pidx := 0; pidx < wpool.ParallelQ; pidx++ {
		for _, localResults := range <-chansResultsBatch[pidx] {
			printResults(localResults)
		}
	}

}

/*
func parallelQueryWorkerSingleQueue(ngdb *NgramDB, wpool *WorkerPool, wpoolStartIdx int, chJobIn chan ParallelQueryJob) {
	for {
		select {
		case job := <-chJobIn:
			if !job.ValidJob {
				job.ChResult <- nil
			} else {
				queryDispatcher(ngdb, wpool, wpoolStartIdx, job.OpQ, job.ChResult)
			}
		}
	}
}
func queryBatchDispatcherSingleQueue(ngdb *NgramDB, wpool *WorkerPool, opQ []OpQuery) {
	defer timeStop(time.Now(), "qdb():"+strconv.Itoa(len(opQ)))

	timeQDB := time.Now()

	qsz := len(opQ)

	chansResults := make([]chan []NgramResult, 0, qsz)
	chanJobs := make(chan ParallelQueryJob, qsz)
	for i := 0; i < qsz; i++ {
		chansResults = append(chansResults, make(chan []NgramResult, 1))
		chanJobs <- ParallelQueryJob{opQ[i], chansResults[i], true}
	}

	for pidx := 0; pidx < wpool.ParallelQ; pidx++ {
		go parallelQueryWorkerSingleQueue(ngdb, wpool, pidx*wpool.Workers, chanJobs)
	}

	// Gather and print results
	for qidx := 0; qidx < qsz; qidx++ {
		localResults := <-chansResults[qidx]
		if localResults != nil {
			printResults(localResults)
		}
	}

	timeSave(timeQDB, "qdb-total")
}
*/
func processWorkload(in *bufio.Reader, ngdb *NgramDB, workerPool *WorkerPool, opIdx int) *NgramDB {
	defer timeStop(time.Now(), "process()")

	opQs := make([]OpQuery, 0, 256)

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
			queryBatchDispatcherRoundBatch(ngdb, workerPool, opQs)
			//queryBatchDispatcherSingleQueue(ngdb, workerPool, opQs)
			opQs = opQs[:0]

			//lowOpIdx = opIdx
			continue
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
	fmt.Fprintf(os.Stderr, "cores[%d] parallelq[%d] workers[%d]\n", runtime.NumCPU(), workerPool.ParallelQ, workerPool.Workers)

	in := bufio.NewReader(os.Stdin)

	ngdb, opIdx := readInitial(in, ngdb)

	ngdb = processWorkload(in, ngdb, workerPool, opIdx)
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
