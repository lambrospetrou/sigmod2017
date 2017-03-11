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
#include "CYUtils.hpp"
#include "cpp_btree/btree_map.h"

#include <iostream>
#include <vector>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <algorithm>
#include <map>
#include <cstring>

////////////////////////////
// CHECKPOINT TYPE L WORKING
///////////////////////////



namespace cy {
namespace trie {

    template<typename K, typename V> 
        using Map = btree::btree_map<K, V>;

    enum class OpType : uint8_t { ADD = 0, DEL = 1 };
    enum class NodeType : uint8_t { S = 0, L = 1, X = 2 };
    constexpr NodeType DEFAULT_NODE_TYPE = NodeType::S;

    constexpr size_t TYPE_S_MAX = 4;
    constexpr size_t TYPE_L_MAX = 256;
    constexpr size_t TYPE_X_DEPTH = 240000;
    
    constexpr size_t MEMORY_POOL_BLOCK_SIZE = 1<<20;
    constexpr size_t MEMORY_POOL_BLOCK_SIZE_L = 1<<10;

    ////////////////////////////////////////// 
    // Forward declarations to compile!
    struct TrieNode_t;
    struct TrieNodeL_t;
    struct TrieNodeS_t;
    struct MemoryPool_t;
    union NodeChild;
    inline static TrieNode_t* _newTrieNode(TrieNode_t*p, size_t=0);
    inline static NodeChild _newTrieNodeS(NodeChild p, size_t depth);
    inline static NodeChild _newTrieNodeL(NodeChild p, size_t depth);
    //////////////////////////////////////////

    /////////////////////////////////////////

    struct RecordHistory {
        struct Record_t {
            size_t OpIdx;
            OpType Type; // 0-Add, 1-Delete

            Record_t() {}
            Record_t(size_t idx, OpType t) : OpIdx(idx), Type(t) {}
        };
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

    /////////////////////////////////////////
    union NodeChild {
        TrieNodeL_t *L;
        TrieNodeS_t *S;
        TrieNode_t *X;

        NodeChild() {}
        NodeChild(std::nullptr_t t) : L(nullptr) {(void)t;}
        NodeChild(TrieNodeL_t *l) : L(l) {}
        NodeChild(TrieNodeS_t *s) : S(s) {}
        NodeChild(TrieNode_t *p) : X(p) {}

        inline operator bool() const {
            return L != nullptr;
        }
    };

    template<size_t SIZE> 
        struct DataS {
            static constexpr size_t INDEX_SIZE = sizeof(uint8_t) * SIZE;

            uint8_t ChildrenIndex[INDEX_SIZE + sizeof(NodeChild*)*SIZE];
            NodeChild* Children;
            size_t Size;

            DataS() : Children(reinterpret_cast<NodeChild*>(ChildrenIndex + INDEX_SIZE)), Size(0) {}
        };


    struct TrieNodeS_t {
        NodeType Type;
        NodeChild _Parent; // TODO check if removing the parent has any effect. We can use the previous iteration cNode to determine it
        RecordHistory State;

        DataS<TYPE_S_MAX> DtS;    
    };
    struct TrieNodeL_t {
        NodeType Type;
        NodeChild _Parent;
        RecordHistory State;

        struct DataL {
            NodeChild Children[256];
        } DtL;
    };

    struct TrieNode_t {
        NodeType Type;
        TrieNode_t *_Parent;

        RecordHistory State; 
        
        // TODO Optimization
        // TODO Create a custom String class that does not copy the contents of the char* for each key
        Map< std::string, TrieNode_t*> ChildrenMap;
    };

    inline static NodeChild _growTypeSWith(const TrieNodeS_t *cNode, const uint8_t pb, const uint8_t cb) {
        auto parent = cNode->_Parent;
        auto newNode = _newTrieNodeL(cNode->_Parent, 0).L;
        
        newNode->State = cNode->State;
        for (size_t cidx=0; cidx<TYPE_S_MAX; ++cidx) {
            cNode->DtS.Children[cidx].S->_Parent = newNode;
            newNode->DtL.Children[cNode->DtS.ChildrenIndex[cidx]] = cNode->DtS.Children[cidx];
        }
        auto childNode = _newTrieNodeS(newNode, 0);
        newNode->DtL.Children[cb] = childNode;
        switch(parent.S->Type) {
        case NodeType::S: 
        {
            auto sp = parent.S;
            for (size_t cidx=0; cidx<sp->DtS.Size; ++cidx) {
                if (sp->DtS.ChildrenIndex[cidx] == pb) {
                    sp->DtS.Children[cidx] = newNode;
                    break;
                }
            }
            break; 
        }
        case NodeType::L:
            parent.L->DtL.Children[pb] = newNode;
            break;
        }
        return childNode;
    }

    /////////////////////////////

    struct MemoryPool_t {
        std::vector<TrieNodeS_t*> _mS;
        size_t allocatedS; // nodes given from the latest block

        std::vector<TrieNodeL_t*> _mL;
        size_t allocatedL; // nodes given from the latest block
        
        MemoryPool_t() : allocatedS(0), allocatedL(0) {
            _mS.reserve(128);
            _mS.push_back(new TrieNodeS_t[MEMORY_POOL_BLOCK_SIZE]);
            
            _mL.reserve(128);
            _mL.push_back(new TrieNodeL_t[MEMORY_POOL_BLOCK_SIZE_L]);
        }

        
        inline TrieNodeL_t* _newNodeL() {
            //return new TrieNodeL_t();
            if (allocatedL >= MEMORY_POOL_BLOCK_SIZE_L) {
                _mL.push_back(new TrieNodeL_t[MEMORY_POOL_BLOCK_SIZE_L]);
                allocatedL = 0;
            }
            return _mL.back() + allocatedL++;
        }
        
        inline TrieNodeS_t* _newNodeS() {
            //return new TrieNode_t();
            if (allocatedS >= MEMORY_POOL_BLOCK_SIZE) {
                _mS.push_back(new TrieNodeS_t[MEMORY_POOL_BLOCK_SIZE]);
                allocatedS = 0;
            }
            return _mS.back() + allocatedS++;
        }
        
        inline TrieNode_t* _new() {
            return new TrieNode_t();
        }
    } MemoryPool;

    inline static TrieNode_t* init(TrieNode_t *node, NodeType t, TrieNode_t* p) {
        node->_Parent = p;
        node->Type = t;
        return node;
    }

    inline static NodeChild _newTrieNodeS(NodeChild p, size_t depth) {
        if (depth < TYPE_X_DEPTH) {
            TrieNodeS_t *node = MemoryPool._newNodeS();
            node->_Parent = p;
            node->Type = NodeType::S;
            return node;
        }
        //return init(MemoryPool._new(), NodeType::X, p);
        abort();
        return (TrieNodeS_t*)nullptr;
    }
    inline static NodeChild _newTrieNodeL(NodeChild p, size_t depth) {
        if (depth < TYPE_X_DEPTH) {
            TrieNodeL_t *node = MemoryPool._newNodeL();
            node->_Parent = p;
            node->Type = NodeType::L;
            std::fill_n(node->DtL.Children, TYPE_L_MAX, nullptr);
            return node;
        }
        //return init(MemoryPool._new(), NodeType::X, p);
        abort();
        return (TrieNodeL_t*)nullptr;
    }
    
    inline static TrieNode_t* _newTrieNode(TrieNode_t*p, size_t depth) {
        if (depth < TYPE_X_DEPTH) {
            return init(MemoryPool._new(), DEFAULT_NODE_TYPE, p);
        }
        return init(MemoryPool._new(), NodeType::X, p);
    }
    


    ////////////////////////////


    // @param s The whole ngram
    static NodeChild AddString(NodeChild cNode, const std::string& s) {
        const size_t bsz = s.size();
        const uint8_t* bs = reinterpret_cast<const uint8_t*>(s.data());

        for (size_t bidx = 0; bidx < bsz; bidx++) {
            const uint8_t cb = bs[bidx];

            switch(cNode.L->Type) {
                case NodeType::S:
                    {
                        auto sNode = cNode.S;
                        size_t cidx;
                        auto& childrenIndex = sNode->DtS.ChildrenIndex;
                        const size_t csz = sNode->DtS.Size;
                        for (cidx = 0; cidx<csz; cidx++) {
                            if (childrenIndex[cidx] == cb) {
                                break;
                            }
                        }
                        if (cidx >= csz) {
                            if (csz == TYPE_S_MAX) {
                                cNode = _growTypeSWith(sNode, bs[bidx-1], cb);
                            } else {
                                sNode->DtS.Children[sNode->DtS.Size++] = _newTrieNodeS(cNode, bidx);
                                childrenIndex[csz] = cb;
                                cNode = sNode->DtS.Children[csz];
                            }
                        } else {
                            cNode = sNode->DtS.Children[cidx];
                        }

                        break;
                    }
                case NodeType::L:
                    {
                        if (!cNode.L->DtL.Children[cb]) {
                            cNode.L->DtL.Children[cb] = _newTrieNodeS(cNode, bidx);
                        }
                        cNode = cNode.L->DtL.Children[cb];
                        break;
                    }
                case NodeType::X:
                    {
                        /*
                        const std::string key(s.substr(bidx));
                        auto it = cNode->ChildrenMap.find(key);
                        if (it == cNode->ChildrenMap.end()) {
                            it = cNode->ChildrenMap.insert(it, {std::move(key), _newTrieNode(cNode)});
                        }
                        cNode = it->second; bidx = bsz;
                        break;
                        */
                        abort();
                    }
                default:
                    abort();
            }
        }

        return cNode;
    }

    // @param s The whole ngram
    static NodeChild FindString(NodeChild cNode, const std::string& s) {
        const size_t bsz = s.size();
        const uint8_t* bs = reinterpret_cast<const uint8_t*>(s.data());

        for (size_t bidx = 0; bidx < bsz; bidx++) {
            const uint8_t cb = bs[bidx];

            switch(cNode.L->Type) {
                case NodeType::S:
                    {
                        auto sNode = cNode.S;
                        size_t cidx;
                        const auto& childrenIndex = sNode->DtS.ChildrenIndex;
                        const size_t csz = sNode->DtS.Size;
                        for (cidx = 0; cidx<csz; cidx++) {
                            if (childrenIndex[cidx] == cb) {
                                break;
                            }
                        }
                        if (cidx >= csz) {
                            return nullptr;
                        }

                        cNode = sNode->DtS.Children[cidx];
                        break;    
                    }
                case NodeType::L:
                    {
                        if (!cNode.L->DtL.Children[cb]) {
                            return nullptr;
                        }
                        cNode = cNode.L->DtL.Children[cb];
                        break;
                    }
                case NodeType::X:
                    {
                        /*
                        const std::string key(s.substr(bidx));
                        auto it = cNode->ChildrenMap.find(key);
                        if (it == cNode->ChildrenMap.end()) {
                            return nullptr;
                        }
                        cNode = it->second; bidx = bsz;
                        break;
                        */
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

        const std::pair<const char*, const char*> firstWord{s+bidx, s+endOfWord};
        const size_t firstWordSz = endOfWord-bidx;

        auto& children = cNode->ChildrenMap;
        auto lb = std::lower_bound(children.begin(), children.end(), firstWord, 
                [firstWordSz](const std::pair<std::string, TrieNode_t*>& ngram, const std::pair<const char*, const char*>& firstWord) { 
                    return std::strncmp(ngram.first.c_str(), firstWord.first, firstWordSz) < 0;
                });

        if (lb == children.end()) {
            //std::cerr << "No Match." << std::endl;
            return std::move(results);
        }

        for (auto it=lb, end=children.end(); it != end; ++it) {
            //std::cerr << "Matching...." << it->first << std::endl;
            // We have to stop as soon as results are not prefixed with firstWord
            
            if (std::strncmp(it->first.c_str(), firstWord.first, firstWordSz) != 0) {
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

            if ((std::strncmp(ngram.c_str()+firstWordSz, firstWord.first+firstWordSz, ngsz-firstWordSz) == 0) && it->second->State.IsValid(opIdx)) {
                results.push_back(bidx + ngsz);
            }
        }

        return std::move(results);
    }

    // @param s The whole doc prefix that we need to find ALL NGRAMS matching
    static std::vector<size_t> FindAll(NodeChild cNode, const char *s, const size_t docSize, int opIdx) {
        const size_t bsz = docSize;
        const uint8_t* bs = reinterpret_cast<const uint8_t*>(s);

        // Holds the endPos for each valid ngram found in the given doc
        std::vector<size_t> results;

        for (size_t bidx = 0; bidx < bsz; bidx++) {
            const uint8_t cb = bs[bidx];

            switch(cNode.L->Type) {
                case NodeType::S:
                    {
                        auto sNode = cNode.S;
                        size_t cidx;
                        const auto& childrenIndex = sNode->DtS.ChildrenIndex;
                        const size_t csz = sNode->DtS.Size;
                        for (cidx = 0; cidx<csz; cidx++) {
                            if (childrenIndex[cidx] == cb) {
                                break;
                            }
                        }
                        if (cidx >= csz) {
                            return std::move(results);
                        }

                        cNode = sNode->DtS.Children[cidx];
                        break;
                    }
                case NodeType::L:
                    {
                        if (!cNode.L->DtL.Children[cb]) {
                            return std::move(results);
                        }
                        cNode = cNode.L->DtL.Children[cb];
                        break;
                    }
                case NodeType::X:
                    {
                        //return _findAllTypeX(cNode, results, s, bidx, bsz, opIdx);
                        abort();
                    }
                default:
                    abort();
            }

            // For Types S,M,L
            // at the end of each word check if the ngram so far is a valid result
            if (bs[bidx+1] == ' ' && cNode.L->State.IsValid(opIdx)) {
                results.push_back(bidx+1);
            }
        }

        // For Types S,M,L
        // We are here it means the whole doc matched the ngram ending at cNode
        if (cNode && cNode.L->State.IsValid(opIdx)) {
            results.emplace_back(bsz);
        }

        return std::move(results);
    }


    size_t NumberOfGrows = 0;
    size_t NumberOfNodes = 0;
    size_t xChMin = 999999, xChMax = 0, xChTotal = 0, xTotal = 0, xCh0 = 0;
    static void _takeAnalytics(NodeChild cNode) {
        NumberOfNodes++;
        /*
        switch(cNode->Type) {
            case NodeType::S:
                {
                    const size_t csz = cNode->DtS.Size;
                    for (size_t cidx = 0; cidx<csz; cidx++) {
                        _takeAnalytics(cNode->DtS.Children[cidx]);
                    }
                    break;
                }
            case NodeType::L:
                {
                    NumberOfGrows++;
                    
                    const auto& children = cNode->DtL.Children;
                    for (size_t cidx = 0; cidx<TYPE_L_MAX; cidx++) {
                        if (children[cidx]) {
                            _takeAnalytics(children[cidx]);
                        }
                    }
                    break;
                }
            case NodeType::X:
                {
                    xTotal++;
                    const size_t chsz = cNode->ChildrenMap.size();
                    xChTotal += chsz;
                    if (chsz == 0) { xCh0++; }
                    if (chsz < xChMin) { xChMin = chsz; }
                    if (chsz > xChMax) { xChMax = chsz; }

                    break;
                }
            default:
                abort();
        }
        */
    }

    struct TrieRoot_t {
        NodeChild Root;

        TrieRoot_t() {
            std::cerr << "S" << TYPE_S_MAX << " L" << TYPE_L_MAX << " X" << TYPE_X_DEPTH << " MEM_BLOCK" << MEMORY_POOL_BLOCK_SIZE << std::endl;
            Root = _newTrieNodeL(nullptr, 0);
            if (!Root) { abort(); }
        }
        ~TrieRoot_t() {
            _takeAnalytics(Root);
            std::cerr << "upgrades::" << NumberOfGrows << " #nodes::" << NumberOfNodes << std::endl;
            std::cerr << "MAP::" << xTotal << "::" << xCh0 << "::" << xChMin << "::" << xChMax << "::" << (xChTotal*1.0/xTotal) << std::endl;
        }
    };


};
};

#endif
