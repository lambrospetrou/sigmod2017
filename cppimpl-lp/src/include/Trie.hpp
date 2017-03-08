#ifndef __CY_TRIE__
#define __CY_TRIE__
/**
 * Statistics (XL dataset):
 * 
 * Amount   | children
 *  ~700M    | <=2
 *  ~2M     | >2
 *  ~400K   | >4
 *  ~175K   | >8
 *
 * */


#pragma once

#include "Timer.hpp"
#include "cpp_btree/btree_map.h"

#include <iostream>
#include <vector>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <algorithm>
#include <map>

namespace cy {
namespace trie {

    template<typename K, typename V> 
        using Map = btree::btree_map<K, V>;

    enum class OpType : uint8_t { ADD = 0, DEL = 1 };
    enum class NodeType : uint8_t { S = 0, L = 1, X = 2 };
    constexpr NodeType DEFAULT_NODE_TYPE = NodeType::S;

    constexpr size_t TYPE_S_MAX = 4;
    constexpr size_t TYPE_L_MAX = 256;
    constexpr size_t TYPE_X_DEPTH = 24;

    size_t NumberOfGrows = 0;
    size_t NumberOfNodes = 0;
    
    struct Record_t {
        size_t OpIdx;
        OpType Type; // 0-Add, 1-Delete

        Record_t() {}
        Record_t(size_t idx, OpType t) : OpIdx(idx), Type(t) {}
    };

    // TODO Optimization
    // TODO Maybe creating an interface for the nodes, and then each type implementing that
    // TODO interface will be faster because you will avoid the SWITCH(TYPE) in ALL the calls.
    // TODO On the other hand, every node jumping will be a function call on that node, so it might be slower too!!!

    
    struct TrieNode_t;

    template<size_t SIZE>
    struct DataS {
        static constexpr size_t INDEX_SIZE = sizeof(uint8_t) * SIZE;

        uint8_t ChildrenIndex[INDEX_SIZE + sizeof(TrieNode_t**)*SIZE];
        TrieNode_t** Children;
        size_t Size;

        DataS() : Children(reinterpret_cast<TrieNode_t**>(ChildrenIndex + INDEX_SIZE)), Size(0) {}
    };

    struct DataL {
        std::vector<TrieNode_t*> Children;
    };

    struct TrieNode_t {
        NodeType Type;
        TrieNode_t *_Parent;

        DataS<TYPE_S_MAX> DtS;
        DataL DtL;
        
        // TODO Optimization
        // TODO Create a custom String class that does not copy the contents of the char* for each key
        Map< std::string, TrieNode_t*> ChildrenMap;



        TrieNode_t() : _Parent(nullptr) { init(DEFAULT_NODE_TYPE); }
        TrieNode_t(NodeType t) : _Parent(nullptr) { init(t); }
        TrieNode_t(TrieNode_t *p) : _Parent(p) { init(DEFAULT_NODE_TYPE); }
        TrieNode_t(NodeType t, TrieNode_t *p) : _Parent(p) { init(t); }

        inline void init(NodeType t) {
            switch (t) {
                case NodeType::S:
                    Type = NodeType::S;
                    break;
                case NodeType::L:
                    Type = NodeType::L;
                    DtL.Children.reserve(TYPE_L_MAX); DtL.Children.resize(TYPE_L_MAX, nullptr);
                    break;
                case NodeType::X:
                    Type = NodeType::X;
                    break;
            }
        }

        inline static TrieNode_t* _new(TrieNode_t*p, size_t depth = 0) {
            NumberOfNodes++;
            if (depth < TYPE_X_DEPTH) {
                return new TrieNode_t(p);
            } else {
                return new TrieNode_t(NodeType::X, p);
            }
        }

        ///////////// Add / Remove ////////////

        TrieNode_t* _growTypeSWith(const uint8_t cb) {
            NumberOfGrows++;
            
            Type = NodeType::L;

            DtL.Children.reserve(TYPE_L_MAX); DtL.Children.resize(TYPE_L_MAX, nullptr);
            for (size_t i=0, sz=TYPE_S_MAX; i<sz; i++) {
                DtL.Children[DtS.ChildrenIndex[i]] = DtS.Children[i];
            }

            TrieNode_t *newNode = new TrieNode_t(this);
            DtL.Children[cb] = newNode;

            return newNode;
        }


        ///////////// Updates related /////////////
        
        std::vector<Record_t> Records; // allow parallel batched queries

        inline void MarkAdd(size_t opIdx) { Records.emplace_back(opIdx, OpType::ADD); }
        inline void MarkDel(size_t opIdx) { Records.emplace_back(opIdx, OpType::DEL); }
        inline bool IsValid(size_t opIdx) const {
            if (Records.empty()) { return false; }
            const size_t rsz = Records.size();

            if (Records[rsz-1].OpIdx < opIdx) {
                return Records[rsz-1].Type == OpType::ADD;
            }
            for (size_t ridx=rsz-1; ridx>0; ridx--) {
                if (Records[ridx-1].OpIdx < opIdx) {
                    return Records[ridx-1].Type == OpType::ADD;
                }
            }
            return false;
        }
    };

    // @param s The whole ngram
    static TrieNode_t* AddString(TrieNode_t *cNode, const std::string& s) {
        const size_t bsz = s.size();
        const uint8_t* bs = reinterpret_cast<const uint8_t*>(s.data());

        for (size_t bidx = 0; bidx < bsz; bidx++) {
            const uint8_t cb = bs[bidx];

            switch(cNode->Type) {
                case NodeType::S:
                    {
                        size_t cidx;
                        auto& childrenIndex = cNode->DtS.ChildrenIndex;
                        const size_t csz = cNode->DtS.Size;
                        for (cidx = 0; cidx<csz; cidx++) {
                            if (childrenIndex[cidx] == cb) {
                                break;
                            }
                        }
                        if (cidx >= csz) {
                            if (csz == TYPE_S_MAX) {
                                cNode = cNode->_growTypeSWith(cb);
                            } else {
                                cNode->DtS.Children[cNode->DtS.Size++] = TrieNode_t::_new(cNode, bidx);
                                childrenIndex[csz] = cb;
                                cNode = cNode->DtS.Children[csz];
                            }
                        } else {
                            cNode = cNode->DtS.Children[cidx];
                        }

                        break;
                    }
                case NodeType::L:
                    {
                        if (!cNode->DtL.Children[cb]) {
                            cNode->DtL.Children[cb] = TrieNode_t::_new(cNode, bidx);
                        }
                        cNode = cNode->DtL.Children[cb];
                        break;
                    }
                case NodeType::X:
                    {
                        const std::string key(s.substr(bidx));
                        auto it = cNode->ChildrenMap.find(key);
                        if (it == cNode->ChildrenMap.end()) {
                            it = cNode->ChildrenMap.insert(it, {std::move(key), TrieNode_t::_new(cNode)});
                        }
                        cNode = it->second; bidx = bsz;
                        break;
                    }
                default:
                    abort();
            }
        }

        return cNode;
    }

    // @param s The whole ngram
    static TrieNode_t* FindString(TrieNode_t* cNode, const std::string& s) {
        const size_t bsz = s.size();
        const uint8_t* bs = reinterpret_cast<const uint8_t*>(s.data());

        for (size_t bidx = 0; bidx < bsz; bidx++) {
            const uint8_t cb = bs[bidx];

            switch(cNode->Type) {
                case NodeType::S:
                    {
                        size_t cidx;
                        const auto& childrenIndex = cNode->DtS.ChildrenIndex;
                        const size_t csz = cNode->DtS.Size;
                        for (cidx = 0; cidx<csz; cidx++) {
                            if (childrenIndex[cidx] == cb) {
                                break;
                            }
                        }
                        if (cidx >= csz) {
                            return nullptr;
                        }

                        cNode = cNode->DtS.Children[cidx];
                        break;
                    }
                case NodeType::L:
                    {
                        if (!cNode->DtL.Children[cb]) {
                            return nullptr;
                        }
                        cNode = cNode->DtL.Children[cb];
                        break;
                    }
                case NodeType::X:
                    {
                        const std::string key(s.substr(bidx));
                        auto it = cNode->ChildrenMap.find(key);
                        if (it == cNode->ChildrenMap.end()) {
                            return nullptr;
                        }
                        cNode = it->second; bidx = bsz;
                        break;
                    }
                default:
                    abort();
            }
        }

        return cNode;
    }

    inline static std::vector<size_t> _findAllTypeX(TrieNode_t* cNode, std::vector<size_t>& results, const char*s, size_t bidx, const size_t bsz, int opIdx) {

        // 1. Find the next word
        // 2. Fetch the range of ngrams matching that word (lower_bound/equal_range)
        // 3. Examine all these ngrams to be prefix of the remaining doc (s)
        // 4. Careful!!! if a full ngram ends in the middle of a doc word

        const uint8_t* bs = reinterpret_cast<const uint8_t*>(s);
        size_t endOfWord = bidx;
        // skip the inbetween spaces and then reach the end of the word (there is at least 1 space or 1 char)
        for (; endOfWord < bsz && bs[endOfWord] == ' '; endOfWord++) {}
        for (; endOfWord < bsz && bs[endOfWord] != ' '; endOfWord++) {}

        const std::string firstWord(s+bidx, s+endOfWord);

        auto& children = cNode->ChildrenMap;
        auto lb = std::lower_bound(children.begin(), children.end(), firstWord, 
                [](const std::pair<std::string, TrieNode_t*>& ngram, const std::string& firstWord) { 
                return ngram.first < firstWord;
                });

        if (lb == children.end()) {
            //std::cerr << "No Match." << std::endl;
            return std::move(results);
        }

        for (auto it=lb, end=children.end(); it != end; ++it) {
            //std::cerr << "Matching...." << it->first << std::endl;
            // We have to stop as soon as results are not prefixed with firstWord
            if (it->first.compare(0, firstWord.size(), firstWord) != 0) {
                //std::cerr << "Not Matching." << std::endl;
                break;
            }

            const auto& ngram = it->first;

            // make sure doc can fit the ngram
            if (ngram.size() > bsz-bidx) { continue; }
            const size_t ngsz = ngram.size();
            const uint8_t* cbs = bs+bidx;
            const size_t cbssz = bsz-bidx;

            // make sure that the ngram is not ending in the middle of a word
            if (ngsz < cbssz && cbs[ngsz] != ' ') { continue; }

            bool wrong = false;
            const uint8_t* ngbs = reinterpret_cast<const uint8_t*>(ngram.data());
            for (size_t ngidx=0; ngidx<ngsz; ++ngidx) {
                if (ngbs[ngidx] != cbs[ngidx]) { wrong = true; break; }
            }
            if (!wrong && it->second->IsValid(opIdx)) {
                results.push_back(bidx + ngsz);
            }
        }

        return std::move(results);
    }

    // @param s The whole doc prefix that we need to find ALL NGRAMS matching
    static std::vector<size_t> FindAll(TrieNode_t* cNode, const char *s, const size_t docSize, int opIdx) {
        const size_t bsz = docSize;
        const uint8_t* bs = reinterpret_cast<const uint8_t*>(s);

        // Holds the endPos for each valid ngram found in the given doc
        std::vector<size_t> results;

        for (size_t bidx = 0; bidx < bsz; bidx++) {
            const uint8_t cb = bs[bidx];

            switch(cNode->Type) {
                case NodeType::S:
                    {
                        size_t cidx;
                        const auto& childrenIndex = cNode->DtS.ChildrenIndex;
                        const size_t csz = cNode->DtS.Size;
                        for (cidx = 0; cidx<csz; cidx++) {
                            if (childrenIndex[cidx] == cb) {
                                break;
                            }
                        }
                        if (cidx >= csz) {
                            return std::move(results);
                        }

                        cNode = cNode->DtS.Children[cidx];
                        break;
                    }
                case NodeType::L:
                    {
                        if (!cNode->DtL.Children[cb]) {
                            return std::move(results);
                        }
                        cNode = cNode->DtL.Children[cb];
                        break;
                    }
                case NodeType::X:
                    {
                        return _findAllTypeX(cNode, results, s, bidx, bsz, opIdx);
                    }
                default:
                    abort();
            }

            // For Types S,M,L
            // at the end of each word check if the ngram so far is a valid result
            if (bs[bidx+1] == ' ' && cNode->IsValid(opIdx)) {
                results.push_back(bidx+1);
            }
        }

        // For Types S,M,L
        // We are here it means the whole doc matched the ngram ending at cNode
        if (cNode && cNode->IsValid(opIdx)) {
            results.emplace_back(bsz);
        }

        return std::move(results);
    }


    struct TrieRoot_t {
        TrieNode_t *Root;

        TrieRoot_t() {
            Root = TrieNode_t::_new(nullptr, 0);
            if (Root == nullptr) { abort(); }
        }
        ~TrieRoot_t() {
            std::cerr << "upgrades::" << NumberOfGrows << " #nodes::" << NumberOfNodes << std::endl;
            delete Root;
        }
    };

};
};

#endif
