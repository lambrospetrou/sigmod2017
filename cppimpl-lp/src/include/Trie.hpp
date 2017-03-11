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

//#define USE_TYPE_X

namespace cy {
namespace trie {

    template<typename K, typename V> 
        using Map = btree::btree_map<K, V>;

    enum class OpType : uint8_t { ADD = 0, DEL = 1 };
    enum class NodeType : uint8_t { S = 0, M = 1, L = 2, X = 3 };

    constexpr size_t TYPE_S_MAX = 4;
    constexpr size_t TYPE_M_MAX = 16;
    constexpr size_t TYPE_L_MAX = 256;
    constexpr size_t TYPE_X_DEPTH = 24;
    
    constexpr size_t MEMORY_POOL_BLOCK_SIZE_S = 1<<20;
    constexpr size_t MEMORY_POOL_BLOCK_SIZE_M = 1<<15;
    constexpr size_t MEMORY_POOL_BLOCK_SIZE_L = 1<<10;
    constexpr size_t MEMORY_POOL_BLOCK_SIZE_X = 1<<10;

    ////////////////////////////////////////// 
    // Forward declarations to compile!
    struct MemoryPool_t;
    
    struct TrieNodeL_t;
    struct TrieNodeS_t;
    struct TrieNodeM_t;
    struct TrieNodeX_t;
    union NodePtr;
    
    inline static NodePtr _newTrieNodeS(NodePtr p);
    inline static NodePtr _newTrieNodeM(NodePtr p);
    inline static NodePtr _newTrieNodeL(NodePtr p);
    inline static NodePtr _newTrieNodeX(NodePtr p);
    inline static NodePtr _newTrieNode(NodePtr p, size_t depth = 0);
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
    union NodePtr {
        TrieNodeS_t *S;
        TrieNodeM_t *M;
        TrieNodeL_t *L;
        TrieNodeX_t *X;

        NodePtr() {}
        NodePtr(std::nullptr_t t) : L(nullptr) {(void)t;}
        NodePtr(TrieNodeS_t *s) : S(s) {}
        NodePtr(TrieNodeM_t *m) : M(m) {}
        NodePtr(TrieNodeL_t *l) : L(l) {}
        NodePtr(TrieNodeX_t *x) : X(x) {}

        inline operator bool() const { return L != nullptr; }
    };

    template<size_t SIZE> 
        struct DataS {
            static constexpr size_t INDEX_SIZE = sizeof(uint8_t) * SIZE;

            uint8_t ChildrenIndex[INDEX_SIZE + sizeof(NodePtr*)*SIZE];
            NodePtr* Children;
            size_t Size;

            DataS() : Children(reinterpret_cast<NodePtr*>(ChildrenIndex + INDEX_SIZE)), Size(0) {}
        };


    struct TrieNodeS_t {
        const NodeType Type = NodeType::S;
        NodePtr _Parent; // TODO check if removing the parent has any effect. We can use the previous iteration cNode to determine it
        RecordHistory State;

        DataS<TYPE_S_MAX> DtS;    
    };
    struct TrieNodeM_t {
        const NodeType Type = NodeType::M;
        NodePtr _Parent;
        RecordHistory State;

        DataS<TYPE_M_MAX> DtM;    
    };
    struct TrieNodeL_t {
        const NodeType Type = NodeType::L;
        NodePtr _Parent;
        RecordHistory State;

        struct DataL {
            NodePtr Children[256];
        } DtL;
    };
    struct TrieNodeX_t {
        const NodeType Type = NodeType::X;
        NodePtr _Parent;
        RecordHistory State; 
        
        // TODO Optimization
        // TODO Create a custom String class that does not copy the contents of the char* for each key
        Map<std::string, NodePtr> ChildrenMap;
    };

    inline static NodePtr _growTypeSWith(const TrieNodeS_t *cNode, const uint8_t pb, const uint8_t cb) {
        auto parent = cNode->_Parent;
        auto newNode = _newTrieNodeM(cNode->_Parent).M;
        
        newNode->State = std::move(cNode->State);
        newNode->DtM.Size = TYPE_S_MAX+1;

        //std::memcpy(newNode->DtM.ChildrenIndex, cNode->DtS.ChildrenIndex, TYPE_S_MAX * sizeof(uint8_t));
        //std::memcpy(newNode->DtM.Children, cNode->DtS.Children, TYPE_S_MAX * sizeof(NodePtr*));        
        for (size_t cidx=0; cidx<TYPE_S_MAX; ++cidx) {
            newNode->DtM.Children[cidx] = cNode->DtS.Children[cidx];
            newNode->DtM.Children[cidx].S->_Parent = newNode;
            newNode->DtM.ChildrenIndex[cidx] = cNode->DtS.ChildrenIndex[cidx];
        }

        auto childNode = _newTrieNode(newNode);
        newNode->DtM.ChildrenIndex[TYPE_S_MAX] = cb;
        newNode->DtM.Children[TYPE_S_MAX] = childNode;

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
        case NodeType::M: 
        {
            auto sp = parent.M;
            for (size_t cidx=0; cidx<sp->DtM.Size; ++cidx) {
                if (sp->DtM.ChildrenIndex[cidx] == pb) {
                    sp->DtM.Children[cidx] = newNode;
                    break;
                }
            }
            break; 
        }
        case NodeType::L:
            parent.L->DtL.Children[pb] = newNode;
            break;
        default:
            abort();
        }
        return childNode;
    }
    inline static NodePtr _growTypeMWith(const TrieNodeM_t *cNode, const uint8_t pb, const uint8_t cb) {
        auto parent = cNode->_Parent;
        auto newNode = _newTrieNodeL(cNode->_Parent).L;
        
        newNode->State = std::move(cNode->State);
        for (size_t cidx=0; cidx<TYPE_M_MAX; ++cidx) {
            cNode->DtM.Children[cidx].M->_Parent = newNode;
            newNode->DtL.Children[cNode->DtM.ChildrenIndex[cidx]] = cNode->DtM.Children[cidx];
        }
        auto childNode = _newTrieNode(newNode);
        newNode->DtL.Children[cb] = childNode;

        // Update the parent
        switch(parent.M->Type) {
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
        case NodeType::M: 
        {
            auto sp = parent.M;
            for (size_t cidx=0; cidx<sp->DtM.Size; ++cidx) {
                if (sp->DtM.ChildrenIndex[cidx] == pb) {
                    sp->DtM.Children[cidx] = newNode;
                    break;
                }
            }
            break; 
        }
        case NodeType::L:
            parent.L->DtL.Children[pb] = newNode;
            break;
        default:
            abort();
        }
        return childNode;
    }

    /////////////////////////////

    struct MemoryPool_t {
        std::vector<TrieNodeS_t*> _mS;
        size_t allocatedS; // nodes given from the latest block

        std::vector<TrieNodeM_t*> _mM;
        size_t allocatedM; // nodes given from the latest block
        
        std::vector<TrieNodeL_t*> _mL;
        size_t allocatedL; // nodes given from the latest block
        
        std::vector<TrieNodeX_t*> _mX;
        size_t allocatedX; // nodes given from the latest block
        
        MemoryPool_t() : allocatedS(0), allocatedL(0), allocatedX(0) {
            _mS.reserve(128);
            _mS.push_back(new TrieNodeS_t[MEMORY_POOL_BLOCK_SIZE_S]);
            
            _mM.reserve(128);
            _mM.push_back(new TrieNodeM_t[MEMORY_POOL_BLOCK_SIZE_M]);
            
            _mL.reserve(128);
            _mL.push_back(new TrieNodeL_t[MEMORY_POOL_BLOCK_SIZE_L]);
#ifdef USE_TYPE_X            
            _mX.reserve(128);
            _mX.push_back(new TrieNodeX_t[MEMORY_POOL_BLOCK_SIZE_X]);
#endif
        }

        inline TrieNodeS_t* _newNodeS() {
            //return new TrieNodeS_t();
            if (allocatedS >= MEMORY_POOL_BLOCK_SIZE_S) {
                _mS.push_back(new TrieNodeS_t[MEMORY_POOL_BLOCK_SIZE_S]);
                allocatedS = 0;
            }
            return _mS.back() + allocatedS++;
        }

        inline TrieNodeM_t* _newNodeM() {
            if (allocatedM >= MEMORY_POOL_BLOCK_SIZE_M) {
                _mM.push_back(new TrieNodeM_t[MEMORY_POOL_BLOCK_SIZE_M]);
                allocatedM = 0;
            }
            return _mM.back() + allocatedM++;
        }
        
        inline TrieNodeL_t* _newNodeL() {
            //return new TrieNodeL_t();
            if (allocatedL >= MEMORY_POOL_BLOCK_SIZE_L) {
                _mL.push_back(new TrieNodeL_t[MEMORY_POOL_BLOCK_SIZE_L]);
                allocatedL = 0;
            }
            return _mL.back() + allocatedL++;
        }
        
        inline TrieNodeX_t* _newNodeX() {
            //return new TrieNodeX_t();
            if (allocatedX >= MEMORY_POOL_BLOCK_SIZE_X) {
                _mX.push_back(new TrieNodeX_t[MEMORY_POOL_BLOCK_SIZE_X]);
                allocatedX = 0;
            }
            return _mX.back() + allocatedX++;
        }
    } MemoryPool;

    inline static NodePtr _newTrieNodeS(NodePtr p) {
        TrieNodeS_t *node = MemoryPool._newNodeS();
        node->_Parent = p;
        return node;
    }
    inline static NodePtr _newTrieNodeM(NodePtr p) {
        TrieNodeM_t *node = MemoryPool._newNodeM();
        node->_Parent = p;
        return node;
    }
    inline static NodePtr _newTrieNodeL(NodePtr p) {
        TrieNodeL_t *node = MemoryPool._newNodeL();
        node->_Parent = p;
        std::fill_n(node->DtL.Children, TYPE_L_MAX, nullptr);
        return node;
    }
    inline static NodePtr _newTrieNodeX(NodePtr p) {
        TrieNodeX_t *node = MemoryPool._newNodeX();
        node->_Parent = p;
        return node;
    }
    inline static NodePtr _newTrieNode(NodePtr p, size_t depth) {
#ifdef USE_TYPE_X
        if (depth < TYPE_X_DEPTH) {
            return _newTrieNodeS(p);
        }
        return _newTrieNodeX(p);
#else
        return _newTrieNodeS(p);
#endif
    }
    
    ////////////////////////////


    // @param s The whole ngram
    static NodePtr AddString(NodePtr cNode, const std::string& s) {
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
                                sNode->DtS.Children[sNode->DtS.Size++] = _newTrieNode(cNode, bidx);
                                childrenIndex[csz] = cb;
                                cNode = sNode->DtS.Children[csz];
                            }
                        } else {
                            cNode = sNode->DtS.Children[cidx];
                        }

                        break;
                    }
                case NodeType::M:
                    {
                        auto mNode = cNode.M;
                        size_t cidx;
                        auto& childrenIndex = mNode->DtM.ChildrenIndex;
                        const size_t csz = mNode->DtM.Size;
                        for (cidx = 0; cidx<csz; cidx++) {
                            if (childrenIndex[cidx] == cb) {
                                break;
                            }
                        }
                        if (cidx >= csz) {
                            if (csz == TYPE_M_MAX) {
                                cNode = _growTypeMWith(mNode, bs[bidx-1], cb);
                            } else {
                                mNode->DtM.Children[mNode->DtM.Size++] = _newTrieNode(mNode, bidx);
                                childrenIndex[csz] = cb;
                                cNode = mNode->DtM.Children[csz];
                            }
                        } else {
                            cNode = mNode->DtM.Children[cidx];
                        }

                        break;
                    }
                case NodeType::L:
                    {
                        if (!cNode.L->DtL.Children[cb]) {
                            cNode.L->DtL.Children[cb] = _newTrieNode(cNode, bidx);
                        }
                        cNode = cNode.L->DtL.Children[cb];
                        break;
                    }
                case NodeType::X:
                    {
                        auto xNode = cNode.X;
                        const std::string key(s.substr(bidx));
                        auto it = xNode->ChildrenMap.find(key);
                        if (it == xNode->ChildrenMap.end()) {
                            it = xNode->ChildrenMap.insert(it, {std::move(key), _newTrieNode(cNode)});
                        }
                        cNode = it->second; bidx = bsz;
                        break;
                        abort();
                    }
                default:
                    abort();
            }
        }

        return cNode;
    }

    // @param s The whole ngram
    static NodePtr FindString(NodePtr cNode, const std::string& s) {
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
                case NodeType::M:
                    {
                        auto mNode = cNode.M;
                        size_t cidx;
                        const auto& childrenIndex = mNode->DtM.ChildrenIndex;
                        const size_t csz = mNode->DtM.Size;
                        for (cidx = 0; cidx<csz; cidx++) {
                            if (childrenIndex[cidx] == cb) {
                                break;
                            }
                        }
                        if (cidx >= csz) {
                            return nullptr;
                        }

                        cNode = mNode->DtM.Children[cidx];
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
                        auto xNode = cNode.X;
                        const std::string key(s.substr(bidx));
                        auto it = xNode->ChildrenMap.find(key);
                        if (it == xNode->ChildrenMap.end()) {
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

    inline static std::vector<size_t> _findAllTypeX(TrieNodeX_t *cNode, std::vector<size_t>& results, const char*s, size_t bidx, const size_t bsz, int opIdx) {

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
                [firstWordSz](const std::pair<std::string, NodePtr>& ngram, const std::pair<const char*, const char*>& firstWord) { 
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

            if ((std::strncmp(ngram.c_str()+firstWordSz, firstWord.first+firstWordSz, ngsz-firstWordSz) == 0) && it->second.S->State.IsValid(opIdx)) {
                results.push_back(bidx + ngsz);
            }
        }

        return std::move(results);
    }

    // @param s The whole doc prefix that we need to find ALL NGRAMS matching
    static std::vector<size_t> FindAll(NodePtr cNode, const char *s, const size_t docSize, int opIdx) {
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
                case NodeType::M:
                    {
                        auto mNode = cNode.M;
                        size_t cidx;
                        const auto& childrenIndex = mNode->DtM.ChildrenIndex;
                        const size_t csz = mNode->DtM.Size;
                        for (cidx = 0; cidx<csz; cidx++) {
                            if (childrenIndex[cidx] == cb) {
                                break;
                            }
                        }
                        if (cidx >= csz) {
                            return std::move(results);
                        }

                        cNode = mNode->DtM.Children[cidx];
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
                        return _findAllTypeX(cNode.X, results, s, bidx, bsz, opIdx);
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


    size_t GrowsM = 0, GrowsL = 0;
    size_t NumberOfNodes = 0;
    size_t xChMin = 999999, xChMax = 0, xChTotal = 0, xTotal = 0, xCh0 = 0;
    static void _takeAnalytics(NodePtr cNode) {
        NumberOfNodes++;
        switch(cNode.S->Type) {
            case NodeType::S:
                {
                    auto sNode = cNode.S;
                    const size_t csz = sNode->DtS.Size;
                    for (size_t cidx = 0; cidx<csz; cidx++) {
                        _takeAnalytics(sNode->DtS.Children[cidx]);
                    }
                    break;
                }
            case NodeType::M:
                {
                    GrowsM++;
                    auto mNode = cNode.M;
                    const size_t csz = mNode->DtM.Size;
                    for (size_t cidx = 0; cidx<csz; cidx++) {
                        _takeAnalytics(mNode->DtM.Children[cidx]);
                    }
                    break;
                }
            case NodeType::L:
                {
                    GrowsL++;
                    const auto& children = cNode.L->DtL.Children;
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
                    const size_t chsz = cNode.X->ChildrenMap.size();
                    xChTotal += chsz;
                    if (chsz == 0) { xCh0++; }
                    if (chsz < xChMin) { xChMin = chsz; }
                    if (chsz > xChMax) { xChMax = chsz; }

                    break;
                }
            default:
                abort();
        }
    }

    struct TrieRoot_t {
        NodePtr Root;

        TrieRoot_t() {
            std::cerr << "S" << TYPE_S_MAX << " L" << TYPE_L_MAX << " X" << TYPE_X_DEPTH;
            std::cerr << " MEM_S" << MEMORY_POOL_BLOCK_SIZE_S;
            std::cerr << " MEM_M" << MEMORY_POOL_BLOCK_SIZE_M;
            std::cerr << " MEM_L" << MEMORY_POOL_BLOCK_SIZE_L;
            std::cerr << " MEM_X" << MEMORY_POOL_BLOCK_SIZE_X;
            std::cerr << std::endl;

            Root = _newTrieNodeL(nullptr);
            if (!Root) { abort(); }
        }
        ~TrieRoot_t() {
            _takeAnalytics(Root);
            std::cerr << "growsM::" << GrowsM << " growsL::" << GrowsL << " #nodes::" << NumberOfNodes << std::endl;
            std::cerr << "MAP::" << xTotal << "::" << xCh0 << "::" << xChMin << "::" << xChMax << "::" << (xChTotal*1.0/xTotal) << std::endl;
        }
    };


};
};

#endif
