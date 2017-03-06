#include "include/Timer.hpp"
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

//#define DEBUG 1

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

struct OpQuery {
    std::string Doc;
	int OpIdx;

    OpQuery() {}
    OpQuery(string d, int idx) : Doc(d), OpIdx(idx) {}
};
struct OpUpdate {
    std::string Ngram;
	int OpIdx;
	uint8_t OpType; // 0-ADD, 1-DEL

    OpUpdate() {}
    OpUpdate(string ngram, int idx, uint8_t t) : Ngram(std::move(ngram)), OpIdx(idx), OpType(t) {}
};

struct NgramDB {

    cy::trie::TrieRoot_t Trie;

    public:

    NgramDB() {}
    
    void AddNgram(const std::string& s, int opIdx) {
        //std::cerr << "a::" << s << std::endl;
	    //auto cNode = Trie.Root->AddString(&Trie.Root, s);
	    auto cNode = cy::trie::AddString(Trie.Root, s);
	    cNode->MarkAdd(opIdx);

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
		    cNode->MarkDel(opIdx);
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
    size_t ParallelQ;
	size_t MaxDocSplit;

    WorkersContext() : ParallelQ(1), MaxDocSplit(-1) {}
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

void queryEvaluation(NgramDB *ngdb, const OpQuery& op) {
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

    outputResults(std::cout, std::move(results));
}

void processWorkload(istream& in, NgramDB *ngdb, WorkersContext *wctx, int opIdx) {
    auto start = timer.getChrono();
    
    uint64_t tA{0}, tD{0}, tQ{0};

    vector<OpQuery> opQs; opQs.reserve(256);
    vector<OpUpdate> opUs; opUs.reserve(256);

    std::string line;
	for (;;) {
		if (!std::getline(in, line)) {
            if (!in.eof()) {
			    std::cerr << "Error" << std::endl;
            }
			break;
		}
		
		opIdx++;

        auto startSingle = timer.getChrono();
        
        char type = line[0];
        switch (type) {
            case 'D':
                //opUs.emplace_back(ss.str(), opIdx, 1);
			    ngdb->RemoveNgram(line.substr(2), opIdx);
                tD += timer.getChrono(startSingle);
                break;
            case 'A':
                //opUs.emplace_back(ss.str(), opIdx, 0);
			    ngdb->AddNgram(line.substr(2), opIdx);
                tA += timer.getChrono(startSingle);
                break;
            case 'Q':
                //opQs.emplace_back(ss.str(), opIdx);
                queryEvaluation(ngdb, std::move(OpQuery{line.substr(2), opIdx}));
                tQ += timer.getChrono(startSingle);
                break;
            case 'F':
                /*
                std::cerr << opQs.size() << ":" << opUs.size() << std::endl;
    
                if (!opQs.empty()) {
                    updatesBatchDispatcher(ngdb, wctx, opUs);
                    opUs.resize(0);
                }

                queryBatchDispatcher(ngdb, wctx, opQs);
                opQs.resize(0);
                */
                break;
        }
	}

	std::cerr << "proc::" << timer.getChrono(start) << ":" << tA << ":" << tD << ":" << tQ << std::endl;
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
		
        //std::cerr << ">" << line << "<" << std::endl;

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

int main() {
	std::ios_base::sync_with_stdio(false);
    setvbuf(stdin, NULL, _IOFBF, 1<<20);

    int opIdx;
    auto start = timer.getChrono();

    std::unique_ptr<NgramDB> ngdb = readInitial(std::cin, &opIdx);

    WorkersContext wctx;
    processWorkload(std::cin, ngdb.get(), &wctx, opIdx);
	
    std::cerr << "main::" << timer.getChrono(start) << std::endl;
}

