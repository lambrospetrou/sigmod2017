#include "include/Timer.hpp"

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

cy::Timer_t timer;

template<typename K, typename V> 
using Map = btree::btree_map<K, V>;
template<typename K, typename V> 
using Set = btree::btree_set<K, V>;

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

    StringSet ngrams;

    public:

    NgramDB() : ngrams() {}
    
    void AddNgram(const std::string s, int opIdx) {
        //std::cerr << "a::" << s << std::endl;
        ngrams.insert(std::move(s));
    }
    
    void RemoveNgram(const std::string& s, int opIdx) {
        //std::cerr << "rem::" << s << std::endl;
        ngrams.erase(s);
    }

    std::vector<std::string> FindNgrams(const std::string& doc, size_t start, size_t opIdx) { 
        std::vector<std::string> r;
        const std::string& prefix = doc.substr(start);
        const size_t psz = prefix.size();
        //std::cerr << "find::" << prefix.substr(0, 20) << std::endl;

        for (const std::string& ngram : ngrams) {
            if (ngram.size() > psz) { continue; }
            if (prefix.compare(0, ngram.size(), ngram) == 0
                && (ngram.size() == psz || prefix[ngram.size()] == ' ') ) {
                //std::cerr << "find::" << prefix.substr(0, 20) << " | " << ngram << std::endl;
                r.push_back(ngram);
            }
        }

        return std::move(r);
    }
};

struct WorkersContext {
    size_t ParallelQ;
	size_t MaxDocSplit;

    WorkersContext() : ParallelQ(1), MaxDocSplit(-1) {}
};

//////////////////////////////////////

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

void queryBatchDispatcher(NgramDB *ngdb, WorkersContext *wctx, std::vector<OpQuery>& opQ) {
    auto start = timer.getChrono();

	size_t qsz{opQ.size()};
	for (size_t qidx = 0; qidx < qsz; qidx++) {
		const auto& op = opQ[qidx];
        queryEvaluation(ngdb, op);
	}
	
    std::cerr << "qbd::" << timer.getChrono(start) << std::endl;
}

void updatesBatchDispatcher(NgramDB *ngdb, WorkersContext *wctx, std::vector<OpUpdate>& opU) {
    auto start = timer.getChrono();

	size_t usz{opU.size()};
	for (size_t uidx = 0; uidx < usz; uidx++) {
		const auto& op = opU[uidx];
		if (op.OpType == 0) {
			ngdb->AddNgram(std::move(op.Ngram), op.OpIdx);
		} else if (op.OpType == 1) {
			ngdb->RemoveNgram(op.Ngram, op.OpIdx);
		}
	}
	
    std::cerr << "ubd::" << timer.getChrono(start) << std::endl;
}

void processWorkload(istream& in, NgramDB *ngdb, WorkersContext *wctx, int opIdx) {
    auto start = timer.getChrono();
    
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

        char type = line[0];
        switch (type) {
            case 'D':
                //opUs.emplace_back(ss.str(), opIdx, 1);
			    ngdb->RemoveNgram(line.substr(2), opIdx);
                break;
            case 'A':
                //opUs.emplace_back(ss.str(), opIdx, 0);
			    ngdb->AddNgram(line.substr(2), opIdx);
                break;
            case 'Q':
                //opQs.emplace_back(ss.str(), opIdx);
                queryEvaluation(ngdb, std::move(OpQuery{line.substr(2), opIdx}));
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

	std::cerr << "proc::" << timer.getChrono(start) << std::endl;
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




/////////////////////////////////////////////////////////////////////////////////

/*
struct Vertex {
	EdgeContainer E;
	uint32_t hashEdges;

public:
	Vertex() : hashEdges(0) {}
};

using VertexContainer = IntMap<Vertex*>;

struct Graph {
	VertexContainer V;
	VertexContainer Vpred;

	void updateTransitiveClosures() {
		std::cerr << "V: " << V.size() << std::endl;

		std::vector<std::pair<int, int>> vs; vs.reserve(V.size());
		for (const auto& vp : V) {
			vs.push_back({vp.first, vp.second->E.size()});
		}

		std::sort(vs.begin(), vs.end(), [](const std::pair<int, int>& a, const std::pair<int, int>& b) {
			return a.second < b.second;
		});

		IntMap<uint32_t> hashed;

		for (const auto& vp : vs) {
			std::cerr << " =" << vp.first << ":" << vp.second << ":";
			hashed[vp.first] = updateTransitiveClosureHash(vp.first, hashed);
		}
	}

	uint32_t updateTransitiveClosureHash(int vid, IntMap<uint32_t>& hashed) {
		std::vector<int> Q; Q.reserve(32);
		IntSet visited;
		visited.insert(vid);
		Q.push_back(vid);
		uint64_t Qidx = 0;

		Vertex *v, *vSource = V[vid];
		int cid;
		while (Qidx < Q.size()) {
			cid = Q[Qidx++];

			v = V[cid];
			for (const auto& k : v->E) {
				auto it = visited.find(k);
				if (it == visited.end()) {
					vSource->hashEdges |= static_cast<uint32_t>(k);

					visited.insert(it, k);
					// required to avoid having vertices that do not exist
					if (V.find(k) != V.end()) {

						if (hashed.find(k) != hashed.end()) {
							//std::cerr << "h";
							vSource->hashEdges |= static_cast<uint32_t>(hashed[k]);
						} else {
							Q.push_back(k);
						}
					}
				}
			}
		}
		return vSource->hashEdges;
	}	

	void updateTransitiveClosureHashFromAdd(int vid) {
		std::vector<int> Q; Q.reserve(32);
		IntSet visited;
		visited.insert(vid);
		Q.push_back(vid);
		uint64_t Qidx = 0;

		Vertex *v;
		int cid;
		while (Qidx < Q.size()) {
			cid = Q[Qidx++];

			v = Vpred[cid];
			for (const auto& k : v->E) {
				auto it = visited.find(k);
				if (it == visited.end()) {
					visited.insert(it, k);
					// required to avoid having vertices that do not exist
					VertexContainer::iterator vit = Vpred.find(k);
					if (vit != Vpred.end()) {
						Q.push_back(k);
						vit->second->hashEdges |= static_cast<uint32_t>(vid);
					}
				}
			}
		}
	}	

	void addEdgeWithTransitiveClosure(int from, int to) {
		VertexContainer::iterator v;
		if ((v = V.find(from)) == V.end()) {
			v = V.insert(v, {from, new Vertex()});
		}
		v->second->E.insert(to);

		if ((v = Vpred.find(to)) == Vpred.end()) {
			v = Vpred.insert(v, {to, new Vertex()});
		}
		v->second->E.insert(from);

		updateTransitiveClosureHashFromAdd(to);
	}

	void addEdge(int from, int to) {
		VertexContainer::iterator v;
		if ((v = V.find(from)) == V.end()) {
			v = V.insert(v, {from, new Vertex()});
		}
		v->second->E.insert(to);

		if ((v = Vpred.find(to)) == Vpred.end()) {
			v = Vpred.insert(v, {to, new Vertex()});
		}
		v->second->E.insert(from);
	}

	void removeEdge(int from, int to) {
		VertexContainer::iterator it;
		if ((it = V.find(from)) != V.end()) {
			it->second->E.erase(to);
		}

		if ((it = Vpred.find(to)) != Vpred.end()) {
			it->second->E.erase(from);
		}
	}

	int biBFS(int from, int to) {
		std::vector<int> QF; QF.reserve(128);
		std::vector<int> QT; QT.reserve(128);

		Map<int, int> visitedF;
		Map<int, int> visitedT;
		
		visitedF[from] = 0;
		size_t QFidx = 0;
		
		visitedT[to] = 0;
		size_t QTidx = 0;

		Vertex *v;
		QF.push_back(from);
		QT.push_back(to);

		while (QFidx < QF.size() && QTidx < QT.size()) {

			if ( (V[QF[QFidx]]->E.size()) < (Vpred[QT[QTidx]]->E.size()) ) {
			//the check now tries to balance visited nodes per BFS
			//if ((QF.size()) < (QT.size())) {
				size_t this_level = QF.size();
				for (;QFidx < this_level;) {
					int vid = QF[QFidx++];

					int newDepth = visitedF[vid] + 1;

					v = V[vid];
					for (const auto& e : v->E) {
						int k = e;
						auto it = visitedF.find(k);
						if (it == visitedF.end()) {
							visitedF.insert(it, {k, newDepth});
							// required to avoid having vertices that do not exist
							if (V.find(k) != V.end()) {
								QF.push_back(k);
							}
						}
						it = visitedT.find(k);
						if (it != visitedT.end()){
							//std::cerr << newDepth << ":" << from << ":" << to << " : " << k << std::endl;
							return it->second + newDepth;
						}
					}
				}
			} else {
				size_t this_level = QT.size();
				for (;QTidx < this_level;) {
					int vid = QT[QTidx++];
					int newDepth = visitedT[vid] + 1;

					v = Vpred[vid];
					for (const auto& e : v->E) {
						int k = e;
						auto it = visitedT.find(k);
						if (it == visitedT.end()) {
							visitedT.insert(it, {k, newDepth});
							// required to avoid having vertices that do not exist
							if (Vpred.find(k) != Vpred.end()) {
								QT.push_back(k);
							}
						}
						it = visitedF.find(k);
						if (it != visitedF.end()){
							return it->second + newDepth;
						}
					}
				}
			}

		}
		return -1;
	}

	int BFS(int from, int to) {
		if (from == to) {
			return 0;
		}
		VertexContainer::iterator v = V.find(from);
		if (V.find(from) == V.end()) {
			return -1;
		}

		if (to > 0 && ((v->second->hashEdges & static_cast<uint32_t>(to)) == 0)) {
			return -1;
		}

		if (Vpred.find(to) == Vpred.end()) {
			return -1;
		}
		//return singleBFS(from, to);
		return biBFS(from, to);
	}
};

std::unique_ptr<Graph> readInitialGraph() {
	int from, to;
	std::string line;
	std::stringstream ss;

	auto G = std::unique_ptr<Graph>(new Graph());
	auto start = timer.getChrono();

	for(;;) {
		if (!std::getline(std::cin, line)) {
			std::cerr << "error" << std::endl;
			break;
		}
		if (line == "S") {
			std::cerr << "loadedInitialGraph:: " << timer.getChrono(start) << std::endl;
			break;
		}
		ss.clear();
		ss.str(line);
		ss >> from >> to;

		G->addEdge(from, to);
	}

	// update the transitive closures
	G->updateTransitiveClosures();
	std::cerr << "finished transitive closure:: " << timer.getChrono(start) << std::endl;

	std::cout << "R\n";

	return std::move(G);
}

uint64_t tQ = 0, tA = 0, tD = 0;
void executeOperation(Graph &G, char c, int from, int to) {
	auto start = timer.getChrono();
	switch (c) {
	case 'Q':
		std::cout << G.BFS(from, to) << std::endl;
		tQ += timer.getChrono(start);
		break;
	case 'A':
		G.addEdgeWithTransitiveClosure(from, to);
		tA += timer.getChrono(start);
		break;
	case 'D':
		G.removeEdge(from, to);
		tD += timer.getChrono(start);
		break;
	}
}

void runExecution(Graph &G) {
	int from, to;
	char c;
	std::string line;
	std::stringstream ss;

	uint64_t start = timer.getChrono();
	uint64_t lastEnd = 0;
	for(;;) {
		//std::cerr << "reading..." << std::endl;
		if (!std::getline(std::cin, line)) {
			break;
		}
		//std::cerr << "read: " << line << std::endl;
		if (line == "F") {
			uint64_t current = timer.getChrono(start) - lastEnd;
			lastEnd += current;
			std::cerr << "runExecutionBatch:: " << current << std::endl;
			continue;
		}
		ss.clear();
		ss.str(line);
		ss >> c >> from >> to;

		//std::cerr << "execute:: " << c << ":" << from << ":" << to << std::endl;
		executeOperation(G, c, from, to);
	}
}

int main() {
	std::ios_base::sync_with_stdio(false);
    setvbuf(stdin, NULL, _IOFBF, 1<<20);

    auto start = timer.getChrono();
    std::unique_ptr<Graph> G = readInitialGraph();
    std::cerr << "readInitialGraph:: " << timer.getChrono(start) << std::endl;

    runExecution(*G);

    std::cerr << "tQ: " << tQ << " tA: " << tA << " tD: " << tD << std::endl;
}
*/
