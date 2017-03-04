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
	"sync"
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
type OpUpdate struct {
	Ngram  string
	OpIdx  int
	OpType byte // 0-ADD, 1-DEL
}

type ParallelQueryBatchJob struct {
	Ngdb        *NgramDB
	Wpool       *WorkerPool
	Pqidx       int
	ActivePQ    int
	OpQ         []OpQuery
	ResultBatch **bytes.Buffer

	Wg *sync.WaitGroup
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

type WorkerPool struct {
	OutWriter *bufio.Writer

	Workers      int
	WorkersTotal int
	ParallelQ    int

	MaxDocSplit int
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
func queryDispatcher(ngdb *NgramDB, wpool *WorkerPool, opQ OpQuery) []NgramResult {
	//defer timeStop(time.Now(), "qd()")

	workers := 1
	doc := opQ.Doc
	opIdx := opQ.OpIdx
	docSz := len(doc)

	//fmt.Fprintln(os.Stderr, "qsz", docSz)

	if docSz == 0 {
		return nil
	}

	if wpool.Workers > 1 {
		if docSz%wpool.MaxDocSplit > 0 {
			workers = docSz/wpool.MaxDocSplit + 1
		} else {
			workers = docSz / wpool.MaxDocSplit
		}
	}

	if workers == 1 {
		return filterResults(partialQueryDoc(ngdb, doc, docSz, opIdx))
	}

	fmt.Fprintln(os.Stderr, "SHOULD NOT COME HERE", docSz)

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
	return filterResults(results)
}

// @master worker
func parallelQueryWorkerRoundBatch(job *ParallelQueryBatchJob) {
	ngdb := job.Ngdb
	wpool := job.Wpool
	pqidx := job.Pqidx
	activePQ := job.ActivePQ
	opQ := job.OpQ
	resultBatch := job.ResultBatch

	//fmt.Fprintln(os.Stderr, "pqw()", pqidx, activePQ, len(opQ))

	// calculate the portion of work
	qsz := len(opQ)
	rounds := qsz / activePQ
	extraRounds := qsz % activePQ

	// Uniformly divide tasks
	var startIdx, endIdx int
	if pqidx < extraRounds {
		startIdx = rounds*pqidx + pqidx
		endIdx = startIdx + rounds + 1
	} else {
		startIdx = rounds*pqidx + extraRounds
		endIdx = startIdx + rounds
	}
	if endIdx > qsz {
		endIdx = qsz
	}

	// Execute batch work and output result to the result channel
	//bResult := bytes.NewBuffer(bytesArr)
	bResult := new(bytes.Buffer)

	// 1st result directly
	for qidx := startIdx; qidx < endIdx; qidx++ {
		lres := queryDispatcher(ngdb, wpool, opQ[qidx])
		outputBytesTo(lres, bResult)
	}

	*resultBatch = bResult
	job.Wg.Done()
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

	var wg sync.WaitGroup
	wg.Add(activePQ)
	resultsBatch := make([]*bytes.Buffer, activePQ, activePQ)

	for i := 0; i < activePQ; i++ {
		//go parallelQueryWorkerRoundBatch(ngdb, wpool, i, activePQ, opQ, chansResultsBatch[i])
		go parallelQueryWorkerRoundBatch(&ParallelQueryBatchJob{ngdb, wpool, i, activePQ, opQ, &resultsBatch[i], &wg})
	}

	wg.Wait()

	// Gather and print results
	for pidx := 0; pidx < activePQ; pidx++ {
		resultsBatch[pidx].WriteTo(wpool.OutWriter)
	}
	wpool.OutWriter.Flush()
}

func updatesBatchDispatcher(ngdb *NgramDB, wpool *WorkerPool, opU []OpUpdate) {
	defer timeSave(time.Now(), "ubd()")

	usz := len(opU)
	for uidx := 0; uidx < usz; uidx++ {
		op := &opU[uidx]
		if op.OpType == 0 {
			ngdb.AddNgram(op.Ngram, op.OpIdx)
		} else if op.OpType == 1 {
			ngdb.RemoveNgram(op.Ngram, op.OpIdx)
		}
	}
}

func processWorkload(in *bufio.Reader, ngdb *NgramDB, workerPool *WorkerPool, opIdx int) *NgramDB {
	defer timeSave(time.Now(), "process()")

	opQs := make([]OpQuery, 0, 256)
	opUs := make([]OpUpdate, 0, 256)

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
			//ngdb.RemoveNgram(line[2:], opIdx)
			opUs = append(opUs, OpUpdate{line[2:], opIdx, byte(1)})
		} else if line[0] == 'A' {
			//ngdb.AddNgram(line[2:], opIdx)
			opUs = append(opUs, OpUpdate{line[2:], opIdx, byte(0)})
		} else if line[0] == 'Q' {
			opQs = append(opQs, OpQuery{line[2:], opIdx})
		} else if line[0] == 'F' {
			if len(opQs) > 0 {
				updatesBatchDispatcher(ngdb, workerPool, opUs)
				opUs = opUs[:0]
			}

			queryBatchDispatcherRoundBatch(ngdb, workerPool, opQs)
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

	workerPool.OutWriter = bufio.NewWriter(os.Stdout)

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

func outputBytesTo(results []NgramResult, b *bytes.Buffer) *bytes.Buffer {
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
func outputBytes(results []NgramResult) *bytes.Buffer {
	var b bytes.Buffer
	return outputBytesTo(results, &b)
}
