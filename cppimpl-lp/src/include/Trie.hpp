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
#include <cstring>
#include <algorithm>
#include <map>
#include <cstring>

#include <emmintrin.h>

//#define LPDEBUG 1

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

    constexpr size_t MEMORY_POOL_BLOCK_SIZE_S = 1<<25;
    constexpr size_t MEMORY_POOL_BLOCK_SIZE_M = 1<<10;
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
            uint8_t ChildrenIndex[sizeof(uint8_t) * SIZE + sizeof(NodePtr*)*SIZE];
            uint8_t Size;

            DataS() : Size(0) {}

            inline NodePtr* Children() { return reinterpret_cast<NodePtr*>(ChildrenIndex + sizeof(uint8_t) * SIZE); }
        };


    struct TrieNodeS_t {
        const NodeType Type = NodeType::S;
        bool Valid;
        std::string Suffix;

        DataS<TYPE_S_MAX> DtS;
    };
    struct TrieNodeM_t {
        const NodeType Type = NodeType::M;
        bool Valid;
        std::string Suffix;

        // 40 bytes so far. To be 16-bit aligned for SIMD we need to pad some bytes
        //uint8_t padding[8];

        DataS<TYPE_M_MAX> DtM;
    } ALIGNED_16;
    struct TrieNodeL_t {
        const NodeType Type = NodeType::L;
        bool Valid;
        std::string Suffix;

        struct DataL {
            NodePtr Children[256];
        } DtL;
    };
    struct TrieNodeX_t {
        const NodeType Type = NodeType::X;
        bool Valid;

        // TODO Optimization
        // TODO Create a custom String class that does not copy the contents of the char* for each key
        Map<std::string, NodePtr> ChildrenMap;
    };


    /////////////////////////////

    struct MemoryPool_t {

    /////////////////////////////////////////
    public:
    ////////////////////////////////////////

        MemoryPool_t() : allocatedS(0), allocatedL(0), allocatedX(0) {
            _mS.reserve(128);
            _mS.push_back(new TrieNodeS_t[MEMORY_POOL_BLOCK_SIZE_S]);

            _mM.reserve(4);
            _mM.push_back(new TrieNodeM_t[MEMORY_POOL_BLOCK_SIZE_M]);

            _mL.reserve(4);
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

        std::vector<TrieNodeS_t*> _mS;
        size_t allocatedS; // nodes given from the latest block

        std::vector<TrieNodeM_t*> _mM;
        size_t allocatedM; // nodes given from the latest block

        std::vector<TrieNodeL_t*> _mL;
        size_t allocatedL; // nodes given from the latest block

        std::vector<TrieNodeX_t*> _mX;
        size_t allocatedX; // nodes given from the latest block
    };

    static inline NodePtr _newTrieNodeS(MemoryPool_t*mem) {
        return mem->_newNodeS();
    }
    static inline NodePtr _newTrieNodeM(MemoryPool_t*mem) {
        return mem->_newNodeM();
    }
    static inline NodePtr _newTrieNodeL(MemoryPool_t*mem) {
        TrieNodeL_t *node = mem->_newNodeL();
        std::memset(node->DtL.Children, 0, TYPE_L_MAX * sizeof(NodePtr*));
        return node;
    }
    static inline NodePtr _newTrieNodeX(MemoryPool_t*mem) {
        return mem->_newNodeX();
    }
    static inline NodePtr _newTrieNodeWithX(MemoryPool_t*mem, size_t depth = 0) {(void)depth;
#ifdef USE_TYPE_X
        if (depth < TYPE_X_DEPTH) {
            return _newTrieNodeS(mem);
        }
        return _newTrieNodeX(mem);
#else
        return _newTrieNodeS(mem);
#endif
    }

    static inline NodePtr _newTrieNode(MemoryPool_t*mem) {
        return _newTrieNodeS(mem);
    }

    ////////////////////////////


    inline static NodePtr _growTypeSWith(MemoryPool_t *mem, TrieNodeS_t *cNode, NodePtr parent, const uint8_t pb, const uint8_t cb, NodePtr nextNode) {
        auto newNode = _newTrieNodeM(mem).M;

        newNode->Valid = cNode->Valid;
        newNode->DtM.Size = TYPE_S_MAX+1;

        for (size_t cidx=0; cidx<TYPE_S_MAX; ++cidx) {
            newNode->DtM.Children()[cidx] = cNode->DtS.Children()[cidx];
            newNode->DtM.ChildrenIndex[cidx] = cNode->DtS.ChildrenIndex[cidx];
        }

        auto childNode = nextNode;
        newNode->DtM.ChildrenIndex[TYPE_S_MAX] = cb;
        newNode->DtM.Children()[TYPE_S_MAX] = childNode;

        switch(parent.S->Type) {
        case NodeType::S:
        {
            auto sp = parent.S;
            for (size_t cidx=0; cidx<sp->DtS.Size; ++cidx) {
                if (sp->DtS.ChildrenIndex[cidx] == pb) {
                    sp->DtS.Children()[cidx] = newNode;
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
                    sp->DtM.Children()[cidx] = newNode;
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
    inline static NodePtr _growTypeMWith(MemoryPool_t *mem, TrieNodeM_t *cNode, NodePtr parent, const uint8_t pb, const uint8_t cb, NodePtr nextNode) {
        auto newNode = _newTrieNodeL(mem).L;

        newNode->Valid = cNode->Valid;
        for (size_t cidx=0; cidx<TYPE_M_MAX; ++cidx) {
            newNode->DtL.Children[cNode->DtM.ChildrenIndex[cidx]] = cNode->DtM.Children()[cidx];
        }
        auto childNode = nextNode;
        newNode->DtL.Children[cb] = childNode;

        // Update the parent
        switch(parent.M->Type) {
        case NodeType::S:
        {
            auto sp = parent.S;
            for (size_t cidx=0; cidx<sp->DtS.Size; ++cidx) {
                if (sp->DtS.ChildrenIndex[cidx] == pb) {
                    sp->DtS.Children()[cidx] = newNode;
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
                    sp->DtM.Children()[cidx] = newNode;
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

    // TODO create special version of the Add that does not require the parent details since most of the time
    // TODO we know that we will not grow since it is new nodes being added!!!
    typedef NodePtr(*AddFunc_t)(NodePtr, const uint8_t, NodePtr, const uint8_t, NodePtr, MemoryPool_t*);
    typedef NodePtr(*SearchFunc_t)(NodePtr, const uint8_t);

    // It might change the *cNode if this node needs to grow to accommodate the new node.
    // @return the added node - nextNode
    static inline NodePtr _doSingleByteAddS(NodePtr cNode, const uint8_t cb, NodePtr nextNode, const uint8_t pb, NodePtr parent, MemoryPool_t *mem) {
        const auto sNode = cNode.S;
        const auto childrenIndex = sNode->DtS.ChildrenIndex;
        const size_t csz = sNode->DtS.Size;
        size_t cidx = 0;
        for (;;) {
            if (cidx >= csz) {
                if (csz == TYPE_S_MAX) {
                    return _growTypeSWith(mem, sNode, parent, pb, cb, nextNode);
                } else {
                    sNode->DtS.Children()[sNode->DtS.Size++] = nextNode;
                    childrenIndex[csz] = cb;
                    return nextNode;
                }
            }
            if (childrenIndex[cidx] == cb) {
                return sNode->DtS.Children()[cidx];
            }
            cidx++;
        }
        return nullptr;
    }
    static inline NodePtr _doSingleByteSearchS(NodePtr cNode, const uint8_t cb) {
        const auto sNode = cNode.S;
        const auto childrenIndex = sNode->DtS.ChildrenIndex;
        const size_t csz = sNode->DtS.Size;
        size_t cidx = 0;
        for (;;) {
            if (cidx >= csz) { return nullptr; }
            if (childrenIndex[cidx] == cb) {
                return sNode->DtS.Children()[cidx];
            }
            cidx++;
        }
        return nullptr;
    }
    // It might change the *cNode if this node needs to grow to accommodate the new node.
    // @return the added node - nextNode
    static inline NodePtr _doSingleByteAddM(NodePtr cNode, const uint8_t cb, NodePtr nextNode, const uint8_t pb, NodePtr parent, MemoryPool_t *mem) {
        const auto mNode = cNode.M;
        const size_t csz = mNode->DtM.Size;
        auto key =_mm_set1_epi8(cb);
        auto cmp =_mm_cmpeq_epi8(key, *(__m128i*)mNode->DtM.ChildrenIndex);
        auto mask=(1<<csz)-1;
        auto bitfield=_mm_movemask_epi8(cmp)&mask;

#ifdef LPDEBUG
        std::cerr << (void*)mNode << ":" << is_aligned(mNode, 16) << std::endl;

        auto& childrenIndex = mNode->DtM.ChildrenIndex;
        size_t tcidx = 0;
        for (tcidx = 0; tcidx<csz; tcidx++) {
            if (childrenIndex[tcidx] == cb) {
                break;
            }
        }
        if (tcidx>=csz && bitfield > 0) {
            std::cerr << (void*)mNode << ":" << is_aligned(mNode, 16) << std::endl;
            std::cerr << (void*)mNode->DtM.ChildrenIndex << ":" << is_aligned(mNode->DtM.ChildrenIndex, 16) << std::endl;
            std::cerr << mask << "::" << bitfield << "::" << csz << "::"<< "::" << tcidx << std::endl;
            abort();
        }
#endif

        if (!bitfield) {
            if (csz == TYPE_M_MAX) {
                return _growTypeMWith(mem, mNode, parent, pb, cb, nextNode);
            } else {
                mNode->DtM.Children()[mNode->DtM.Size++] = nextNode;
                mNode->DtM.ChildrenIndex[csz] = cb;
                return nextNode;
            }
        } else {
            return mNode->DtM.Children()[__builtin_ctz(bitfield)];
        }

        return nullptr;
    }
    static inline NodePtr _doSingleByteSearchM(NodePtr cNode, const uint8_t cb) {
        const auto mNode = cNode.M;
        const size_t csz = mNode->DtM.Size;

        auto key =_mm_set1_epi8(cb);
        auto cmp =_mm_cmpeq_epi8(key, *(__m128i*)mNode->DtM.ChildrenIndex);
        auto mask=(1<<csz)-1;
        auto bitfield=_mm_movemask_epi8(cmp)&mask;
        if (!bitfield) {
            return nullptr;
        }
        return mNode->DtM.Children()[__builtin_ctz(bitfield)];
    }
    // It might change the *cNode if this node needs to grow to accommodate the new node.
    // @return the added node - nextNode
    static inline NodePtr _doSingleByteAddL(NodePtr cNode, const uint8_t cb, NodePtr nextNode, const uint8_t pb, NodePtr parent, MemoryPool_t *mem) {
        (void)pb; (void)parent; (void)mem;
        cNode.L->DtL.Children[cb] = nextNode;
        return nextNode;
    }
    static inline NodePtr _doSingleByteSearchL(NodePtr cNode, const uint8_t cb) {
        return cNode.L->DtL.Children[cb];
    }

    static inline NodePtr _doAddString(MemoryPool_t *mem, NodePtr cuNode, const uint8_t*bs, const size_t bsz, const size_t bidx, NodePtr parent, bool *done, AddFunc_t _doSingleByteAdd, SearchFunc_t _doSingleByteSearch) {
        const uint8_t cb = bs[bidx];
        const uint8_t pb = bidx > 0 ? bs[bidx-1] : 0;

        // The type we use here SHOULD NOT MATTER since this is just for accessing common
        // fields like Suffix and Valid and Type.
        auto cNode = cuNode.S;

        if (cNode->Suffix.empty()) { // We just need to check children
            NodePtr nextNode = _doSingleByteSearch(cNode, cb); // Generic call
            if (!nextNode) {
                nextNode = _newTrieNode(mem);
                if (bidx+1 < bsz) { // this is NOT the last byte so add the remaining as suffix
                    nextNode.L->Suffix = std::move(std::string(bs+bidx+1, bs+bsz));
                    *done = true;
                }
                _doSingleByteAdd(cuNode, cb, nextNode, pb, parent, mem); // Generic call
            }
            // If this is the last byte of the ngram mark its node as valid
            if (bidx+1 == bsz) {
                nextNode.L->Valid = true;
                *done = true;
            }
            return nextNode;
        }

        // We are at a LEAF with suffix
        const auto& suffix = cNode->Suffix;
        const size_t sufsz = suffix.size();
        const uint8_t *sufbs = reinterpret_cast<const uint8_t*>(cNode->Suffix.data());
        size_t common = 0; for (;common < bsz-bidx && common < sufsz && sufbs[common] == bs[bidx+common];) { ++common; }

        if (common == sufsz) { // the new ngram matched the whole existing suffix
            if (common == bsz-bidx) { // we are already at the proper node - don't do anything
                *done = true;
                return cNode;
            }
            // there is some part of the new ngram to be added so we need to create the nodes
            // to cover the common bytes and then we will add as suffix the remaining part of the new ngram

            // Check if there is a common > 0 and then do the 1st child using the generic Add given as parameter
            // then in the for loop use the type S add since all the others are new nodes.
            NodePtr nextNode = cNode;
            if (common > 0) {
                nextNode = _doSingleByteAdd(nextNode, sufbs[0], _newTrieNode(mem), pb, parent, mem); // Generic call
            }
            for (size_t sidx=1; sidx<common; ++sidx) {
                nextNode = _doSingleByteAddS(nextNode, sufbs[sidx], _newTrieNode(mem), pb, parent, mem);
            }
            nextNode.S->Valid = true; // this is for the existing ngram
            nextNode.S->Suffix = std::move(std::string((char*)bs+bidx+common, (char*)bs+bsz)); // the new ngram

            cNode->Suffix = ""; // reset the cNode suffix since now its suffix became normal nodes
            *done = true;
            return nextNode;
        }

        // the common characters are less than suffix size which means we have
        // some existing ngram common but we will need to create 2 new nodes for the rest of the existing ngram
        // and the rest of the new ngram.

        // Check if there is a common > 0 and then do the 1st child using the generic Add given as parameter
        // then in the for loop use the type S add since all the others are new nodes.
        NodePtr nextNode = cNode;
        if (common > 0) {
            nextNode = _doSingleByteAdd(nextNode, sufbs[0], _newTrieNode(mem), pb, parent, mem); // Generic call
        }
        for (size_t sidx=1; sidx<common; ++sidx) {
            nextNode = _doSingleByteAddS(nextNode, sufbs[sidx], _newTrieNode(mem), pb, parent, mem);
        }
        // add the remaining of the existing ngram
        NodePtr newNode;
        if (common > 0) {
            newNode = _doSingleByteAddS(nextNode, sufbs[common], _newTrieNode(mem), pb, parent, mem);
        } else {
            newNode = _doSingleByteAdd(nextNode, sufbs[common], _newTrieNode(mem), pb, parent, mem); // Generic call
        }
        if (common+1 == sufsz) {
            // there was only 1 byte remaining and it was added through a new node.
            newNode.S->Valid = true;
        } else {
            newNode.S->Suffix = std::move(suffix.substr(common+1));
        }

        // add the remaining of the new ngram
        if (bidx+common < bsz) {
            if (common > 0) {
                newNode = _doSingleByteAddS(nextNode, bs[bidx+common], _newTrieNode(mem), pb, parent, mem);
            } else {
                newNode = _doSingleByteAdd(nextNode, bs[bidx+common], _newTrieNode(mem), pb, parent, mem); // Generic call
            }
            if (bidx+common+1 == bsz) {
                // there was only 1 byte remaining and it was added through a new node.
                newNode.S->Valid = true;
            } else {
                newNode.S->Suffix =std::move(std::string((char*)bs+bidx+common+1, (char*)bs+bsz));
            }
            nextNode = newNode;
        } else {
            // the common was the whole new ngram
            nextNode.S->Valid = true;
        }

        cNode->Suffix = ""; // reset the cNode suffix since now its suffix became normal nodes
        *done = true;
        return nextNode;
    }

    // @param s The whole ngram
    static void AddString(MemoryPool_t *mem, NodePtr cNode, const std::string& s) {
        const size_t bsz = s.size();
        const uint8_t* bs = reinterpret_cast<const uint8_t*>(s.data());
        bool done = false;
        NodePtr parent = (TrieNodeS_t*)nullptr;
        NodePtr previous = (TrieNodeS_t*)nullptr;
        for (size_t bidx = 0; bidx < bsz; bidx++) {
            previous = cNode;

            switch(cNode.L->Type) {
                case NodeType::S:
                    {
                        cNode = _doAddString(mem, cNode, bs, bsz, bidx, parent, &done, _doSingleByteAddS, _doSingleByteSearchS);
                        break;
                    }
                case NodeType::M:
                    {
                        cNode = _doAddString(mem, cNode, bs, bsz, bidx, parent, &done, _doSingleByteAddM, _doSingleByteSearchM);
                        break;
                    }
                case NodeType::L:
                    {
                        cNode = _doAddString(mem, cNode, bs, bsz, bidx, parent, &done, _doSingleByteAddL, _doSingleByteSearchL);
                        break;
                    }
                case NodeType::X:
                    {
                        const auto xNode = cNode.X;
                        const std::string key(s.substr(bidx));
                        auto it = xNode->ChildrenMap.find(key);
                        if (it == xNode->ChildrenMap.end()) {
                            it = xNode->ChildrenMap.insert(it, {std::move(key), _newTrieNode(mem)});
                        }
                        done = true;
                        break;
                        abort();
                    }
                default:
                    abort();
            }
            if (done) { return; }

            parent = previous;
        }
    }

    static inline NodePtr _doDelString(NodePtr cuNode, const uint8_t*bs, const size_t bsz, const size_t bidx, bool *done, SearchFunc_t _doSingleByteSearch) {
        const uint8_t cb = bs[bidx];
        const auto cNode = cuNode.S; // SHOULD NOT MATTER which type I take!!!

        if (cNode->Suffix.empty()) { // NOT LEAF
            NodePtr nextNode = _doSingleByteSearch(cNode, cb);
            if (!nextNode) {
                *done = true;
                return nullptr;
            }
            if (bidx+1 == bsz) {
                nextNode.S->Valid = false; // make the delete
                *done = true;
            }
            return nextNode;
        }

        // We are at a LEAF with suffix
        const auto& suffix = cNode->Suffix;
        const size_t sufsz = suffix.size();
        if (sufsz != bsz-bidx) { // we need to match exactly with the whole ngram
            *done = true;
            return nullptr;
        }
        const uint8_t *sufbs = reinterpret_cast<const uint8_t*>(cNode->Suffix.data());
        size_t common = 0;
        for (;common < bsz-bidx && common < sufsz && sufbs[common] == bs[bidx+common];) { ++common; }

        if (common == sufsz) { // the ngram matched the deleted ngram exactly
            cNode->Suffix = ""; // reset the cNode suffix to simulate DELETE
            *done = true;
            return nullptr;
        }
        // Not the same ngram so nothing to delete - just return and end the loop
        *done = true;
        return nullptr;
    }

    // @param s The whole ngram
    static void DelString(NodePtr cNode, const std::string& s) {
        const size_t bsz = s.size();
        const uint8_t* bs = reinterpret_cast<const uint8_t*>(s.data());

        bool done = false;

        for (size_t bidx = 0; bidx < bsz; bidx++) {
            switch(cNode.L->Type) {
                case NodeType::S:
                    {
                        cNode = _doDelString(cNode, bs, bsz, bidx, &done, _doSingleByteSearchS);
                        break;
                    }
                case NodeType::M:
                    {
                        cNode = _doDelString(cNode, bs, bsz, bidx, &done, _doSingleByteSearchM);
                        break;
                    }
                case NodeType::L:
                    {
                        cNode = _doDelString(cNode, bs, bsz, bidx, &done, _doSingleByteSearchL);
                        break;
                    }
                case NodeType::X:
                    {
                        const auto xNode = cNode.X;
                        const std::string key(s.substr(bidx));
                        const auto it = xNode->ChildrenMap.find(key);
                        if (it == xNode->ChildrenMap.end()) { return; }
                        done = true;
                        break;
                    }
                default:
                    abort();
            }
            if (done) { return; }
        }

        return;
    }

    inline static std::vector<std::pair<size_t, uint64_t>> _findAllTypeX(TrieNodeX_t *cNode, std::vector<std::pair<size_t,uint64_t>>& results, const char*s, size_t bidx, const size_t bsz) {

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

            if ((std::strncmp(ngram.c_str()+firstWordSz, firstWord.first+firstWordSz, ngsz-firstWordSz) == 0) && it->second.S->Valid) {
                results.emplace_back(bidx + ngsz, (uint64_t)it->second.L);
            }
        }

        return std::move(results);
    }

    // @return the pointer to the next node to visit or nullptr if we finished and need to return the results
    static NodePtr _doFindAll(NodePtr cuNode, const uint8_t cb, const size_t bsz, const uint8_t *bs, const size_t bidx, std::vector<std::pair<size_t, uint64_t>>& results, SearchFunc_t _doSingleByteSearch) {
        const auto cNode = cuNode.S; // SHOULD NOT MATTER WHAT TYPE YOU GET
        if (cNode->Suffix.empty()) {
            return _doSingleByteSearch(cNode, cb);
        } else {
            // the doc has to match the whole ngram suffix
            const auto suffix = reinterpret_cast<const uint8_t*>(cNode->Suffix.data());
            const auto sufsz = cNode->Suffix.size();
            if (sufsz > bsz-bidx) { return nullptr; }
            if (std::memcmp(suffix, bs+bidx, sufsz) != 0) { return nullptr; }
            const size_t nbidx = bidx + sufsz;
            if (nbidx >= bsz || bs[nbidx] == ' ') {
                results.emplace_back(nbidx, (uint64_t)suffix);
            }
            return nullptr;
        }
    }

    // @param s The whole doc prefix that we need to find ALL NGRAMS matching
    static std::vector<std::pair<size_t, uint64_t>> FindAll(NodePtr cNode, const char *s, const size_t docSize) {
        const size_t bsz = docSize;
        const uint8_t* bs = reinterpret_cast<const uint8_t*>(s);

        // Holds the endPos for each valid ngram found in the given doc and the identifier for the ngram (pointer for now)
        std::vector<std::pair<size_t, uint64_t>> results;

        for (size_t bidx = 0; bidx < bsz; bidx++) {
            const uint8_t cb = bs[bidx];

            switch(cNode.L->Type) {
                case NodeType::S:
                    {
                        cNode = _doFindAll(cNode, cb, bsz, bs, bidx, results, _doSingleByteSearchS);
                        if (!cNode) { return std::move(results); }
                        break;
                    }
                case NodeType::M:
                    {
                        cNode = _doFindAll(cNode, cb, bsz, bs, bidx, results, _doSingleByteSearchM);
                        if (!cNode) { return std::move(results); }
                        break;
                    }
                case NodeType::L:
                    {
                        cNode = _doFindAll(cNode, cb, bsz, bs, bidx, results, _doSingleByteSearchL);
                        if (!cNode) { return std::move(results); }
                        break;
                    }
                case NodeType::X:
                    {
                        return _findAllTypeX(cNode.X, results, s, bidx, bsz);
                        abort();
                    }
                default:
                    abort();
            } // end of switch

            // For Types S,M,L
            // at the end of each word check if the ngram so far is a valid result
            if (bs[bidx+1] == ' ' && cNode.L->Valid) {
                results.emplace_back(bidx+1, (uint64_t)cNode.L);
            }
        }

        // For Types S,M,L
        // We are here it means the whole doc matched the ngram ending at cNode
        //if (cNode && cNode.L->State.IsValid(opIdx)) {
        if (cNode && cNode.L->Valid) {
            results.emplace_back(bsz, (uint64_t)cNode.L);
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
                        _takeAnalytics(sNode->DtS.Children()[cidx]);
                    }
                    break;
                }
            case NodeType::M:
                {
                    GrowsM++;
                    auto mNode = cNode.M;
                    const size_t csz = mNode->DtM.Size;
                    for (size_t cidx = 0; cidx<csz; cidx++) {
                        _takeAnalytics(mNode->DtM.Children()[cidx]);
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


    bool printed = false;
    struct TrieRoot_t {
        NodePtr Root;
        MemoryPool_t MemoryPool;

        TrieRoot_t() {
            if (!printed) {
            std::cerr << sizeof(TrieNodeS_t) << "::" << sizeof(TrieNodeM_t) <<  "::" << sizeof(TrieNodeL_t) <<  "::" << sizeof(TrieNodeX_t) << "::" << sizeof(DataS<2>) << "::" << sizeof(DataS<16>) << "::" << sizeof(NodePtr) << std::endl;

            std::cerr << "S" << TYPE_S_MAX << " L" << TYPE_L_MAX << " X" << TYPE_X_DEPTH;
            std::cerr << " MEM_S" << MEMORY_POOL_BLOCK_SIZE_S;
            std::cerr << " MEM_M" << MEMORY_POOL_BLOCK_SIZE_M;
            std::cerr << " MEM_L" << MEMORY_POOL_BLOCK_SIZE_L;
            std::cerr << " MEM_X" << MEMORY_POOL_BLOCK_SIZE_X;
            std::cerr << std::endl;

            printed = true;
            }
            Root = _newTrieNodeL(&MemoryPool);
            if (!Root) { abort(); }
        }
        ~TrieRoot_t() {
            _takeAnalytics(Root);
            std::cerr << "growsM::" << GrowsM << " growsL::" << GrowsL << " #nodes::" << NumberOfNodes << std::endl;
            //std::cerr << "MAP::" << xTotal << "::" << xCh0 << "::" << xChMin << "::" << xChMax << "::" << (xChTotal*1.0/xTotal) << std::endl;
        }
    };

    inline static void AddNgram(TrieRoot_t *trie, const std::string& s) {
        //std::cerr << "a::" << s << std::endl;
        cy::trie::AddString(&trie->MemoryPool, trie->Root, s);
    }

    inline static void RemoveNgram(TrieRoot_t*trie, const std::string& s) {
        //std::cerr << "rem::" << s << std::endl;
        cy::trie::DelString(trie->Root, s);
    }

};
};

#endif
