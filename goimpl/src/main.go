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
	Workers        int
	ParallelQ      int
	WorkerJobCh    []chan WorkerJob
	WorkerStatusCh []chan WorkerStatus
}

func NewWorkerPool() *WorkerPool {
	parallelq := 2
	numWorkers := runtime.NumCPU()
	// normalize the inner workers to avoid over-threading
	//numWorkers = numWorkers/parallelq + numWorkers%parallelq
	numWorkers = 1

	pool := &WorkerPool{
		Workers:        numWorkers,
		ParallelQ:      parallelq,
		WorkerJobCh:    make([]chan WorkerJob, 0, numWorkers),
		WorkerStatusCh: make([]chan WorkerStatus, 0, numWorkers),
	}
	for i := 0; i < numWorkers*parallelq; i++ {
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

func queryDispatcher(ngdb *NgramDB, wpool *WorkerPool, wpoolStartIdx int, opQ OpQuery, chResult chan []NgramResult) {
	//defer timeSave(time.Now(), "qd()")

	//timeDispatch := time.Now()

	doc := opQ.Doc
	opIdx := opQ.OpIdx
	docSz := len(doc)

	currentWorker := wpoolStartIdx
	cores := wpool.Workers
	chans := make([]chan []NgramResult, 0, cores)

	// TODO Estimate this better based on the NGDB
	maxLenNgram := 100

	// The DIV might less than cores so add 1 to cover all the doc
	batchSz := int(docSz/cores) + 1
	start := 0
	for start < docSz {
		startIdxEnd := int(math.Min(float64(start+batchSz), float64(docSz)))
		// make sure we are at the end of a word
		for ; startIdxEnd < docSz && doc[startIdxEnd] != ' '; startIdxEnd += 1 {
		}

		docEnd := int(math.Min(float64(startIdxEnd+maxLenNgram), float64(docSz)))

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

	//timeSave(timeDispatch, "qd-dispatch")

	//timeResults := time.Now()

	results := make([]NgramResult, 0, 16)
	for _, ch := range chans {
		results = append(results, (<-ch)...)
	}
	//timeSave(timeResults, "qd-results")

	chResult <- results
}

type ParallelQueryJob struct {
	OpQ      OpQuery
	ChResult chan []NgramResult
	ValidJob bool
}

func parallelQueryWorkerRoundRobin(ngdb *NgramDB, wpool *WorkerPool, wpoolStartIdx int, chJobIn chan []ParallelQueryJob) {
	for {
		select {
		case jobs := <-chJobIn:
			timeStart := time.Now()
			for _, job := range jobs {
				if !job.ValidJob {
					job.ChResult <- nil
				} else {
					queryDispatcher(ngdb, wpool, wpoolStartIdx, job.OpQ, job.ChResult)
				}
			}
			timeSave(timeStart, "w()")
		}
	}
}

func queryBatchDispatcherRoundRobin(ngdb *NgramDB, wpool *WorkerPool, opQ []OpQuery) {
	defer timeStop(time.Now(), "qdb():"+strconv.Itoa(len(opQ)))

	qsz := len(opQ)
	rounds := qsz / wpool.ParallelQ
	if qsz%wpool.ParallelQ > 0 {
		rounds += 1
	}

	batchedJobs := make([][]ParallelQueryJob, 0, wpool.ParallelQ)

	chansResults := make([]chan []NgramResult, 0, wpool.ParallelQ)
	chansJobs := make([]chan []ParallelQueryJob, 0, wpool.ParallelQ)
	for i := 0; i < wpool.ParallelQ; i++ {
		chansResults = append(chansResults, make(chan []NgramResult, rounds))
		chansJobs = append(chansJobs, make(chan []ParallelQueryJob, rounds))

		batchedJobs = append(batchedJobs, make([]ParallelQueryJob, 0, rounds))

		go parallelQueryWorkerRoundRobin(ngdb, wpool, i*wpool.Workers, chansJobs[i])
	}

	for qidx := 0; qidx < qsz; {
		for parallelidx := 0; parallelidx < wpool.ParallelQ; parallelidx, qidx = parallelidx+1, qidx+1 {
			if qidx < qsz {
				batchedJobs[parallelidx] = append(batchedJobs[parallelidx], ParallelQueryJob{opQ[qidx], chansResults[parallelidx], true})
			} else {
				batchedJobs[parallelidx] = append(batchedJobs[parallelidx], ParallelQueryJob{OpQuery{}, chansResults[parallelidx], false})
			}
		}
	}

	for i := 0; i < wpool.ParallelQ; i++ {
		chansJobs[i] <- batchedJobs[i]
	}

	// Gather and print results
	for qidx := 0; qidx < qsz; {
		for parallelidx := 0; parallelidx < wpool.ParallelQ; parallelidx, qidx = parallelidx+1, qidx+1 {
			localResults := <-chansResults[parallelidx]
			if localResults != nil {
				printResults(localResults)
			}
		}
	}
}

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
			queryBatchDispatcherRoundRobin(ngdb, workerPool, opQs)
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
