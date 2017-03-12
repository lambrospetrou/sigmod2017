#include "include/Timer.hpp"
#include "include/CYUtils.hpp"
#include "include/Trie.hpp"

#include "include/cpp_btree/btree_map.h"
#include "include/cpp_btree/btree_set.h"

#include <cstdint>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <memory>
#include <new>
#include <sstream>
#include <string>
#include <algorithm>
#include <cassert>

#include <omp.h>

#define USE_OPENMP
#define USE_PARALLEL

cy::Timer_t timer;

template<typename K, typename V> using Map = btree::btree_map<K, V>;
template<typename K, typename V> using Set = btree::btree_set<K, V>;

template<typename T>
using IntMap = btree::btree_map<int, T>;
//using IntMap = std::unordered_map<int, T>;
using IntSet = btree::btree_set<int>;
using StringSet = btree::btree_set<std::string>;

using namespace std;

////////////// TYPES //////////////

enum OpType_t : uint8_t { ADD = 0, DEL = 1, Q = 2 };

struct OpQuery {
    std::string Doc;
	int OpIdx;

    OpQuery() {}
    OpQuery(string d, int idx) : Doc(d), OpIdx(idx) {}
};
struct OpUpdate {
    std::string Ngram;
	int OpIdx;
	OpType_t OpType;

    OpUpdate() {}
    OpUpdate(string ngram, int idx, OpType_t t) : Ngram(std::move(ngram)), OpIdx(idx), OpType(t) {}
};

struct Op_t {
    std::string Line;
	int OpIdx;
	OpType_t OpType;

    Op_t() {}
    Op_t(string l, int idx, OpType_t t) : Line(std::move(l)), OpIdx(idx), OpType(t) {}
};

struct NgramDB {

    cy::trie::TrieRoot_t Trie;

    public:

    NgramDB() {}
    
    void AddNgram(const std::string& s, int opIdx) {
        //std::cerr << "a::" << s << std::endl;
	    //auto cNode = Trie.Root->AddString(&Trie.Root, s);
	    auto cNode = cy::trie::AddString(Trie.Root, s);
	    cNode.L->State.MarkAdd(opIdx);

#ifdef DEBUG
        if (cNode != cy::trie::FindString(Trie.Root, s)) {
            std::cerr << "add or find is wrong!" << std::endl;
            abort();
        }
#endif
    }
    
    void RemoveNgram(const std::string& s, int opIdx) {
        //std::cerr << "rem::" << s << std::endl;
	    auto cNode = cy::trie::FindString(Trie.Root, s);
	    if (cNode) {
		    cNode.L->State.MarkDel(opIdx);
	    }
    }

    // TODO Optimization
    // TODO Return a vector of pairs [start, end) for each ngram matched and the NGRAM's IDX (create this at each trie node)
    // TODO this way we do not copy strings so many times and the uniqueness when printing the results will use the ngram IDX
    // TODO for super fast hashing in the visited set, whereas now we use strings as keys!!!
    std::vector<std::string> FindNgrams(const std::string& doc, size_t docStart, size_t opIdx) { 
        // TODO Use char* directly to the doc to avoid copying
        std::vector<std::string> results;
        
        const auto& ngramResults = cy::trie::FindAll(Trie.Root, doc.data()+docStart, doc.size()-docStart, opIdx);
        for (const auto& ngramPos : ngramResults) {
            results.push_back(doc.substr(docStart, ngramPos));
        }

        return std::move(results);
    }
};

struct WorkersContext {
    size_t NumThreads;

    WorkersContext() : NumThreads(1) {}
};

//////////////////////////////////////

// TODO Optimization - See above to use ngram [start, end) pairs and ngram IDs
void outputResults(std::ostream& out, const std::vector<std::string>& results) {
    if (results.empty()) {
        out << "-1" << std::endl;
        return;
    }

    // Filter results
    StringSet visited;

    std::stringstream ss;
    ss << results[0];
    visited.insert(results[0]);
    for (size_t i=1,sz=results.size(); i<sz; ++i) {
        const auto& ngram = results[i];
        auto it = visited.find(ngram);
        if (it == visited.end()) {
            visited.insert(it, ngram);
            ss << "|" << ngram;
        }
    }
	ss << std::endl;
    
    out << ss.str();
}

std::vector<std::string> queryEvaluationWithResults(NgramDB *ngdb, const OpQuery& op) {
    const auto& doc = op.Doc;
    const size_t opIdx = op.OpIdx;
    size_t start{0}, end{0}, sz{doc.size()};

    size_t startIdxEnd = sz;

    std::vector<std::string> results;

    for (; start < sz; ) {
		// find start of word
		for (start = end; start < startIdxEnd && doc[start] == ' '; ++start) {}
        if (start >= startIdxEnd) { break; }

        std::vector<std::string> cresult = ngdb->FindNgrams(doc, start, opIdx);
        if (!cresult.empty()) {
            results.insert(results.end(), cresult.begin(), cresult.end());
        }
		
        for (end = start; end < sz && doc[end] != ' '; ++end) {}
    }

    return std::move(results);
}
inline void queryEvaluation(NgramDB *ngdb, const OpQuery& op) {
    outputResults(std::cout, std::move(queryEvaluationWithResults(ngdb, op)));
}

uint64_t timeFindAll = 0;
void queryEvaluationInterleaved(NgramDB *ngdb, const OpQuery& op, std::vector<std::vector<std::string>>& gResult) {
    const size_t pidx = omp_get_thread_num();
    const size_t nthreads = omp_get_num_threads();

    const auto& doc = op.Doc;
    const size_t opIdx = op.OpIdx;
    size_t start{0}, end{0}, sz{doc.size()};

    size_t startIdxEnd = sz;

    std::vector<std::pair<size_t, std::vector<std::string>>> local; local.reserve(sz/(nthreads*10));

    size_t wordidx = 0;
    for (; start < sz; ) {
		// find start of word
		for (start = end; start < startIdxEnd && doc[start] == ' '; ++start) {}
        if (start >= startIdxEnd) { break; }

        if ((uint8_t(doc[start]) % nthreads) == pidx) {
            //#pragma omp critical
            //std::cerr << pidx << ":" << wordidx << std::endl;
        
            //gResult[wordidx] = std::move(ngdb->FindNgrams(doc, start, opIdx));
            auto tS = timer.getChrono();
            auto r = std::move(ngdb->FindNgrams(doc, start, opIdx));
            timeFindAll += timer.getChrono(tS);
            if (!r.empty()) {
                local.emplace_back(wordidx, std::move(r));
            }
        }
        
        wordidx++;
        for (end = start; end < sz && doc[end] != ' '; ++end) {}
    }

    #pragma omp critical
    {
        for (const auto& ri : local) { gResult[ri.first] = std::move(ri.second); }
    }
}
uint64_t serialTime = 0;
void queryBatchEvaluation(NgramDB *ngdb, WorkersContext *wctx, const std::vector<OpQuery> *opQs, std::vector<std::vector<std::vector<std::string>>> *gResults, std::vector<size_t> *gResultsFinished) {
    const size_t qsz = opQs->size(); //std::cerr << qsz << "_";
    
    const size_t nthreads = omp_get_num_threads();
    const size_t pidx = omp_get_thread_num();
    //#pragma omp critical
    //std::cerr << pidx << "::" << nthreads << "::" << qsz << std::endl;
    
    uint64_t localTime;
    
    // prepare the results array - THERE IS IMPLICIT BARRIER
    #pragma omp single
    {
        localTime = timer.getChrono();
        
        gResultsFinished->resize(qsz);
        std::memset(gResultsFinished->data(), 0, qsz*sizeof(size_t));
        
        gResults->resize(0);
        gResults->resize(qsz);

        for (size_t qidx=0; qidx<qsz; qidx++) {
            const auto& doc = (*opQs)[qidx].Doc;
            const size_t numWordsApprox = std::count(doc.data(), doc.data()+doc.size(), ' ') + 1;
            //std::cerr << pidx << "::" << qidx << "::words:" << numWordsApprox << "_" << doc.size() << std::endl;
            (*gResults)[qidx].resize(numWordsApprox);
        }
        
        serialTime += timer.getChrono(localTime);
    }

    size_t qidx = 0;
    for(;;) { 

        queryEvaluationInterleaved(ngdb, (*opQs)[qidx], (*gResults)[qidx]);

        // TODO REMOVE THIS
        //#pragma omp barrier
        
        bool iam = false;
        #pragma omp critical
        {
            if (++(*gResultsFinished)[qidx] == nthreads) { iam = true; }
        }

        // print the results
        //#pragma omp single nowait
        //#pragma omp master
        if (iam) {
            //std::cerr << pidx <<  "::started printing::" << qidx << std::endl;
            localTime = timer.getChrono();

            std::vector<std::string> cresult; cresult.reserve((*opQs)[qidx].Doc.size()/10);
            for (const auto& wr : (*gResults)[qidx]) {
                if (!wr.empty()) {
                    cresult.insert(cresult.end(), wr.begin(), wr.end());
                }
            }
            outputResults(std::cout, std::move(cresult));

            serialTime += timer.getChrono(localTime);
            //std::cerr << pidx <<  "::finished printing::" << qidx << std::endl;
        }
        
        if (++qidx >= qsz) { return; }
    }
}

void processUpdatesBatch(NgramDB *ngdb, WorkersContext *wctx, const std::vector<OpUpdate> *opUs) {
    (void)wctx;
    const size_t opsz = opUs->size();
    for (size_t opidx=0; opidx<opsz; ++opidx) {
        const auto& u = (*opUs)[opidx];
        switch(u.OpType) {
        case OpType_t::ADD:
            ngdb->AddNgram(u.Ngram, u.OpIdx);
            break;
        case OpType_t::DEL:
            ngdb->RemoveNgram(u.Ngram, u.OpIdx);
            break;
        default: abort();
        }
    }
}

uint64_t timeReading = 0;
bool readNextBatch(istream& in, NgramDB *ngdb, vector<OpQuery>& opQs, vector<OpUpdate>& opUs, vector<Op_t>& Q, int& opIdx) {
    auto start = timer.getChrono();
    std::string line;
    for (;;) {
        if (!std::getline(in, line)) {
            if (!in.eof()) { std::cerr << "Error" << std::endl; }
            timeReading += timer.getChrono(start);
            return true;
            break;
        }
        opIdx++;

        char type = line[0];
        switch (type) {
            case 'D':
                opUs.emplace_back(line.substr(2), opIdx, OpType_t::DEL);
                //Q.emplace_back(line.substr(2), opIdx, OpType_t::DEL);
                //ngdb->RemoveNgram(line.substr(2), opIdx);
                //tD += timer.getChrono(startSingle);
                break;
            case 'A':
                opUs.emplace_back(line.substr(2), opIdx, OpType_t::ADD);
                //Q.emplace_back(line.substr(2), opIdx, OpType_t::ADD);
                //ngdb->AddNgram(line.substr(2), opIdx);
                //tA += timer.getChrono(startSingle);
                break;
            case 'Q':
                opQs.emplace_back(line.substr(2), opIdx);
                //Q.emplace_back(line.substr(2), opIdx, OpType_t::Q);
                //queryEvaluation(ngdb, std::move(OpQuery{line.substr(2), opIdx}));
                //tQ += timer.getChrono(startSingle);
                break;
            case 'F':
                timeReading += timer.getChrono(start);
                return false;
                break;
        }
    } // end of current batch (or single command for serial)

    // should never come here!
    abort();
}

void processWorkload(istream& in, NgramDB *ngdb, WorkersContext *wctx, int opIdx) {
    auto start = timer.getChrono();

    uint64_t tA{0}, tD{0}, tQ{0};

    std::vector<std::vector<std::vector<std::string>>> gResults;
    std::vector<size_t> gResultsFinished;
    
    vector<OpQuery> opQs; opQs.reserve(256);
    vector<OpUpdate> opUs; opUs.reserve(256);
    vector<Op_t> Q; Q.reserve(256);
    std::string line;

    bool exit = false;
    uint64_t timerStart;

#pragma omp parallel shared(ngdb, wctx, exit, Q, opUs, opQs, gResults, tA, tD, tQ)
{
    //@worker - loop
	for (;;) {
        
        #pragma omp master
        {
            exit = readNextBatch(in, ngdb, opQs, opUs, Q, opIdx);
	        //std::cerr << opUs.size() << "::" << opQs.size() << "_";
        }

        #pragma omp barrier
        if (exit) { break; }

        if (opQs.empty()) { continue; }

        #pragma omp master
        { 
            auto tU = timer.getChrono();
            processUpdatesBatch(ngdb, wctx, &opUs); 
            tA += timer.getChrono(tU);
        }
        #pragma omp barrier

        #pragma omp master
        { timerStart = timer.getChrono(); }

        // @parallel
        if (!opQs.empty()) {
            queryBatchEvaluation(ngdb, wctx, &opQs, &gResults, &gResultsFinished);
        }
        #pragma omp barrier

        #pragma omp master
        { 
            opUs.resize(0); opQs.resize(0);
            tQ += timer.getChrono(timerStart);
        }
        /*
        const size_t opsz = Q.size();
        for (size_t opidx=0; opidx<opsz; ++opidx) {
            auto startSingle = timer.getChrono();
            const auto& cop = Q[opidx];
            switch(cop.OpType) {
            case OpType_t::ADD:
			    ngdb->AddNgram(cop.Line, cop.OpIdx);
                tA += timer.getChrono(startSingle);
                break;
            case OpType_t::DEL:
			    ngdb->RemoveNgram(cop.Line, cop.OpIdx);
                tD += timer.getChrono(startSingle);
                break;
            case OpType_t::Q:
                queryEvaluation(ngdb, std::move(OpQuery{cop.Line, cop.OpIdx}));
                tQ += timer.getChrono(startSingle);
                break;
            }
        }// processed all operations
        */

    }// end of outermost loop - exit program
}
	std::cerr << "proc::" << timer.getChrono(start) << ":" << tA << ":" << tD << ":" << tQ << ":" << timeReading << std::endl;
	std::cerr << "proc::" << tQ << ":" << serialTime << " findAll:" << timeFindAll << std::endl;
}

std::unique_ptr<NgramDB> readInitial(std::istream& in, int *outOpIdx) {
    auto start = timer.getChrono();
	
    auto ngdb = std::unique_ptr<NgramDB>(new NgramDB());
    int opIdx{0};

	std::string line;
	for (;;) {
		if (!std::getline(in, line)) {
			std::cerr << "error" << std::endl;
			break;
		}
		
		if (line == "S") {
			std::cerr << "init::" << timer.getChrono(start) << std::endl;
            std::cout << "R" << std::endl;
			break;
		}

        opIdx += 1;
		ngdb->AddNgram(line, opIdx);
	}

    *outOpIdx = opIdx;
	return std::move(ngdb);
}

int main(int argc, char**argv) {
    size_t threads = 1;
    if (argc>1) {
        threads = std::max(atoi(argv[1]), 1);
    }

#ifdef USE_OPENMP
    omp_set_dynamic(0);
    omp_set_num_threads(threads);
    
    std::cerr << "affinity::" << omp_get_proc_bind() << " threads::" << threads <<std::endl;
#endif

	std::ios_base::sync_with_stdio(false);
    setvbuf(stdin, NULL, _IOFBF, 1<<20);

    int opIdx;
    auto start = timer.getChrono();

    std::unique_ptr<NgramDB> ngdb = readInitial(std::cin, &opIdx);

    WorkersContext wctx;
    wctx.NumThreads = threads;
    processWorkload(std::cin, ngdb.get(), &wctx, opIdx);
	
    std::cerr << "main::" << timer.getChrono(start) << std::endl;
}

