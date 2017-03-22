#include "include/Timer.hpp"
#include "include/CYUtils.hpp"
#include "include/Trie.hpp"

#include "include/cpp_btree/btree_map.h"
#include "include/cpp_btree/btree_set.h"

#include <cstdio>
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
//#define USE_PARALLEL

cy::Timer_t timer;

template<typename K, typename V> using Map = btree::btree_map<K, V>;
template<typename K, typename V> using Set = btree::btree_set<K, V>;

template<typename T>
using IntMap = btree::btree_map<int, T>;
//using IntMap = std::unordered_map<int, T>;
using IntSet = btree::btree_set<int>;
using UInt64Set = btree::btree_set<uint64_t>;
using StringSet = btree::btree_set<std::string>;

using namespace std;

////////////// TYPES //////////////

enum OpType_t : uint8_t { ADD = 0, DEL = 1, Q = 2 };

struct Op_t {
    std::string Line;
    OpType_t OpType;

    Op_t() {}
    Op_t(string l, OpType_t t) : Line(std::move(l)), OpType(t) {}
};

struct Result_t {
    const char* start;
    const char* end;
    uint64_t ngramIdx;

    Result_t() {}
    Result_t(const char* s, const char*e, uint64_t id) : start(s), end(e), ngramIdx(id) {}
};

struct NgramDB {

    cy::trie::TrieRoot_t Trie;

    public:

    NgramDB() {}

    inline void AddNgram(const std::string& s) {
        //std::cerr << "a::" << s << std::endl;
        cy::trie::AddNgram(&Trie, s);
    }

    inline void RemoveNgram(const std::string& s) {
        //std::cerr << "rem::" << s << std::endl;
        cy::trie::RemoveNgram(&Trie, s);
    }

    // @param doc The part of the doc to find all matching ngrams that have doc (or part of doc) as their prefix.
    inline void FindNgrams(const std::string& doc, size_t docStart, std::vector<Result_t>& results) {
        const char*docStr = doc.data();
        const auto& ngramResults = cy::trie::FindAll(Trie.Root, doc.data()+docStart, doc.size()-docStart);
        for (const auto& ngramPos : ngramResults) {
            results.emplace_back(docStr+docStart, docStr+docStart+ngramPos.first, ngramPos.second);
        }
    }
};

// Data for each thread
struct ThreadData_t {
    NgramDB *Ngdb;

    ThreadData_t() {
        Ngdb = new NgramDB();
    }
    ~ThreadData_t() {
        if (Ngdb) { delete Ngdb; }
    }
};

struct GResult_t {
    std::vector<Result_t> Results;
    size_t ThreadsDone;

    GResult_t() : ThreadsDone(0) {}
};

struct WorkersContext {
    size_t NumThreads;
    std::vector<ThreadData_t> ThreadData;

    std::vector<GResult_t> GResults; // will have NumOfQs size (1 position for each Q in a batch)

    WorkersContext() {}
    WorkersContext(const size_t nthreads) {
        NumThreads = nthreads;
        ThreadData.resize(nthreads);
    }
};

//////////////////////////////////////
/*
#define MULT 31
static inline bool decider(const char*p, const size_t nthreads, const size_t pidx) {
    size_t h = 0;
    for(;*p && *p != ' '; p++) { h = MULT * h + *p; }
    return (h % nthreads) == pidx;
}
static inline size_t deciderIdx(const char*p, const size_t nthreads) {
    size_t h = 0;
    for(;*p && *p != ' '; p++) { h = MULT * h + *p; }
    return h % nthreads;
}
*/
/*
static inline bool decider(const char*p, const size_t nthreads, const size_t pidx) {
    const size_t h = (*p % (nthreads<<1));
    return (h>>1) == pidx;
}
static inline size_t deciderIdx(const char*p, const size_t nthreads) {
    return (*p % (nthreads<<1))>>1;
}
*/
// Single byte hash assignment
static inline bool decider(const char*p, const size_t nthreads, const size_t pidx) {
    return (*p % nthreads) == pidx;
}
static inline size_t deciderIdx(const char*p, const size_t nthreads) {
    return *p % nthreads;
}

void outputResults(std::ostream& out, const std::vector<Result_t>& results) {
    if (results.empty()) {
        out << "-1\n";
        return;
    }

    // Filter results
    UInt64Set visited;

    std::stringstream ss;
    ss << std::move(std::string(results[0].start, results[0].end-results[0].start));
    //out << std::move(std::string(results[0].start, results[0].end-results[0].start));

    visited.insert(results[0].ngramIdx);
    for (size_t i=1,sz=results.size(); i<sz; ++i) {
        const auto& ngram = results[i];

        //std::cerr << "ngram: " << (uint64_t)ngram.start << "-" << (uint64_t)ngram.end << std::endl;

        auto it = visited.find(ngram.ngramIdx);
        if (it == visited.end()) {
            visited.insert(it, ngram.ngramIdx);
            ss << "|" << std::move(std::string(ngram.start, ngram.end-ngram.start));
            //out << "|" << std::move(std::string(ngram.start, ngram.end-ngram.start));
        }
    }
    ss << "\n";
    out << ss.str();
}

//std::vector<Result_t> queryEvaluationWithResults(NgramDB *ngdb, const OpQuery& op) {
std::vector<Result_t> queryEvaluationWithResults(NgramDB *ngdb, const std::string& doc) {
    uint8_t nthreads = 1;
    uint8_t pidx = 0;
#ifdef USE_OPENMP
    nthreads = omp_get_num_threads();
    pidx = omp_get_thread_num();
    //std::cerr << "pidx::" << pidx << " threads::" << nthreads <<std::endl;
#endif
    //const auto decider = [=](const uint8_t byte){ return (byte % nthreads) == pidx; };

    const auto docPtr = doc.data();
    const size_t sz{doc.size()};
    size_t start{0}, end{0};
    std::vector<Result_t> results;

    for (; start < sz; ) {
        // find start of word
        for (start = end; start < sz && doc[start] == ' '; ++start) {}
        if (start >= sz) { break; }

        //if (decider(doc[start])) {
        if (decider(docPtr + start, nthreads, pidx)) {
            ngdb->FindNgrams(doc, start, results);
        }

        for (end = start; end < sz && doc[end] != ' '; ++end) {}
    }

    return std::move(results);
}
/*
inline void queryEvaluation(NgramDB *ngdb, const OpQuery& op) {
    outputResults(std::cout, std::move(queryEvaluationWithResults(ngdb, op)));
}
*/
inline void queryEvaluationWithAggregation(NgramDB *ngdb, WorkersContext *wctx, const size_t qIdx, const std::string& Doc) {
    auto tresults = std::move(queryEvaluationWithResults(ngdb, Doc));
    bool iShouldPrint = false;
    auto& gresults = wctx->GResults[qIdx].Results;

    #pragma omp critical
    {
        gresults.insert(gresults.end(), tresults.begin(), tresults.end());
        iShouldPrint = ++(wctx->GResults[qIdx].ThreadsDone) == wctx->NumThreads;
    }

    if (iShouldPrint) {
        //std::cerr << "printing pidx::" << omp_get_thread_num() << " threads::" << omp_get_num_threads() <<std::endl;
        // sort the results based on position in the doc and then print
        std::sort(gresults.begin(), gresults.end(), [](const Result_t& l, const Result_t& r) {
            if (l.start < r.start) { return true; }
            if (l.start > r.start) { return false; }
            return l.end < r.end;
        });
        outputResults(std::cout, gresults);
    }
}

uint64_t timeReading = 0;
uint64_t tA{0}, tD{0}, tQ{0};
bool readNextBatch(istream& in, WorkersContext *wctx, vector<Op_t>& Q) {
    auto start = timer.getChrono();
    std::string line;

    size_t numOfQs = 0;

    for (;;) {
        if (!std::getline(in, line)) {
            if (!in.eof()) { std::cerr << "Error" << std::endl; }
            timeReading += timer.getChrono(start);
            return true;
            break;
        }

        auto startSingle = timer.getChrono();
        char type = line[0];
        switch (type) {
            case 'A':
                Q.emplace_back(line.substr(2), OpType_t::ADD);
                //ngdb->AddNgram(line.substr(2), opIdx);
                tA += timer.getChrono(startSingle);
                break;
            case 'D':
                Q.emplace_back(line.substr(2), OpType_t::DEL);
                //ngdb->RemoveNgram(line.substr(2), opIdx);
                tD += timer.getChrono(startSingle);
                break;
            case 'Q':
                Q.emplace_back(line.substr(2), OpType_t::Q);
                numOfQs++;
                //queryEvaluation(ngdb, std::move(OpQuery{line.substr(2), opIdx}));
                tQ += timer.getChrono(startSingle);
                break;
            case 'F':
                wctx->GResults.resize(0); wctx->GResults.resize(numOfQs);
                timeReading += timer.getChrono(start);
                return false;
                break;
        }
    } // end of current batch (or single command for serial)

    // should never come here!
    abort();
}
void queryBatchEvaluationSingle(WorkersContext *wctx, const std::vector<Op_t>& Q) {
    uint8_t nthreads = 1;
    uint8_t pidx = 0;
#ifdef USE_OPENMP
    nthreads = omp_get_num_threads();
    pidx = omp_get_thread_num();
    //std::cerr << "pidx::" << (int)pidx << " threads::" << (int)nthreads <<std::endl;
#endif

    const auto ngdb = wctx->ThreadData[pidx].Ngdb;

    size_t qidx = 0;

    for (const auto& cop : Q) {
        auto startSingle = timer.getChrono();

        switch(cop.OpType) {
        case OpType_t::ADD:
            if (decider(cop.Line.data(), nthreads, pidx)) {
                ngdb->AddNgram(cop.Line);
            }
            tA += timer.getChrono(startSingle);
            break;
        case OpType_t::DEL:
            if (decider(cop.Line.data(), nthreads, pidx)) {
                ngdb->RemoveNgram(cop.Line);
            }
            tD += timer.getChrono(startSingle);
            break;
        case OpType_t::Q:
            queryEvaluationWithAggregation(ngdb, wctx, qidx++, cop.Line);
            tQ += timer.getChrono(startSingle);
            break;
        }
    }// processed all operations
}
void processWorkloadSingle(istream& in, WorkersContext *wctx) {
    auto start = timer.getChrono();

    vector<Op_t> Q; Q.reserve(256);

    //@master - loop
    for (;;) {

        // @master
        if (readNextBatch(in, wctx, Q)) {
            break;
        }

        if (!Q.empty()) {
            // @workers
            #pragma omp parallel shared(wctx, Q)
            {
                queryBatchEvaluationSingle(wctx, Q);
            }

            // @master
            Q.resize(0);
        }

    }// end of outermost loop - exit program
    std::cerr << "proc::" << timer.getChrono(start) << ":" << tA << ":" << tD << ":" << tQ << " reads:" << timeReading << std::endl;
}
static void readInitial(std::istream& in, WorkersContext *wctx) {
    auto start = timer.getChrono();

    const size_t nthreads = wctx->NumThreads;

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

        wctx->ThreadData[deciderIdx(line.data(), nthreads)].Ngdb->AddNgram(line);
    }
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

    auto start = timer.getChrono();

    WorkersContext wctx(threads);

    readInitial(std::cin, &wctx);

    processWorkloadSingle(std::cin, &wctx);

    std::cerr << "main::" << timer.getChrono(start) << std::endl;
}

