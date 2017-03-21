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

//#include <omp.h>

//#define USE_OPENMP
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

    void AddNgram(const std::string& s, int opIdx) {
        //std::cerr << "a::" << s << std::endl;
        cy::trie::AddNgram(&Trie, s, opIdx);
    }

    void RemoveNgram(const std::string& s, int opIdx) {
        //std::cerr << "rem::" << s << std::endl;
        cy::trie::RemoveNgram(&Trie, s, opIdx);
    }

    // TODO Optimization
    // TODO Return a vector of pairs [start, end) for each ngram matched and the NGRAM's IDX (create this at each trie node)
    // TODO this way we do not copy strings so many times and the uniqueness when printing the results will use the ngram IDX
    // TODO for super fast hashing in the visited set, whereas now we use strings as keys!!!
    std::vector<Result_t> FindNgrams(const std::string& doc, size_t docStart, size_t opIdx) {
        // TODO Use char* directly to the doc to avoid copying
        std::vector<Result_t> results;
        const char*docStr = doc.data();
        const auto& ngramResults = cy::trie::FindAll(Trie.Root, doc.data()+docStart, doc.size()-docStart, opIdx);
        for (const auto& ngramPos : ngramResults) {
            results.emplace_back(docStr+docStart, docStr+docStart+ngramPos.first, ngramPos.second);
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

std::vector<Result_t> queryEvaluationWithResults(NgramDB *ngdb, const OpQuery& op) {
    const auto& doc = op.Doc;
    const size_t opIdx = op.OpIdx;
    size_t start{0}, end{0}, sz{doc.size()};

    size_t startIdxEnd = sz;

    std::vector<Result_t> results;

    for (; start < sz; ) {
		// find start of word
		for (start = end; start < startIdxEnd && doc[start] == ' '; ++start) {}
        if (start >= startIdxEnd) { break; }

        auto cresult = ngdb->FindNgrams(doc, start, opIdx);
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
uint64_t tA{0}, tD{0}, tQ{0};
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

        auto startSingle = timer.getChrono();
        char type = line[0];
        switch (type) {
            case 'A':
                //opUs.emplace_back(line.substr(2), opIdx, OpType_t::ADD);
                //Q.emplace_back(line.substr(2), opIdx, OpType_t::ADD);
                ngdb->AddNgram(line.substr(2), opIdx);
                tA += timer.getChrono(startSingle);
                break;
            case 'D':
                //opUs.emplace_back(line.substr(2), opIdx, OpType_t::DEL);
                //Q.emplace_back(line.substr(2), opIdx, OpType_t::DEL);
                ngdb->RemoveNgram(line.substr(2), opIdx);
                tD += timer.getChrono(startSingle);
                break;
            case 'Q':
                //opQs.emplace_back(line.substr(2), opIdx);
                //Q.emplace_back(line.substr(2), opIdx, OpType_t::Q);
                queryEvaluation(ngdb, std::move(OpQuery{line.substr(2), opIdx}));
                tQ += timer.getChrono(startSingle);
                break;
            case 'F':
                //timeReading += timer.getChrono(start);
                return false;
                break;
        }
    } // end of current batch (or single command for serial)

    // should never come here!
    abort();
}

void queryBatchEvaluationSingle(NgramDB *ngdb, WorkersContext *wctx, const std::vector<Op_t>& Q, const std::vector<OpQuery>& opQs) {
    /*
    const size_t qsz = opQs.size();
    for (size_t qidx=0; qidx<qsz; ++qidx) {
        const auto& cop = opQs[qidx];
        queryEvaluation(ngdb, std::move(OpQuery{cop.Doc, cop.OpIdx}));
    }
    */

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
}
void processWorkloadSingle(istream& in, NgramDB *ngdb, WorkersContext *wctx, int opIdx) {
    auto start = timer.getChrono();

    vector<OpQuery> opQs; opQs.reserve(256);
    vector<OpUpdate> opUs; opUs.reserve(256);
    vector<Op_t> Q; Q.reserve(256);
    std::string line;

    //@worker - loop
	for (;;) {

        if (readNextBatch(in, ngdb, opQs, opUs, Q, opIdx)) {
            break;
        }
        if (!Q.empty()) {
            queryBatchEvaluationSingle(ngdb, wctx, Q, opQs);
            Q.resize(0);
        }
        /*
        if (opQs.empty()) { continue; }

        auto tAs = timer.getChrono();
        processUpdatesBatch(ngdb, wctx, &opUs);
        tA += timer.getChrono(tAs);

        auto tQs = timer.getChrono();
        if (!opQs.empty()) {
            queryBatchEvaluationSingle(ngdb, wctx, opQs);
        }
        opUs.resize(0); opQs.resize(0);
        tQ += timer.getChrono(tQs);
        */
    }// end of outermost loop - exit program
	std::cerr << "proc::" << timer.getChrono(start) << ":" << tA << ":" << tD << ":" << tQ << " reads:" << timeReading << std::endl;
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

    processWorkloadSingle(std::cin, ngdb.get(), &wctx, opIdx);

    std::cerr << "main::" << timer.getChrono(start) << std::endl;
}

