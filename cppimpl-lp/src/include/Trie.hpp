#ifndef __CY_TRIE__
#define __CY_TRIE__

#pragma once

#include "Timer.hpp"
#include "cpp_btree/btree_map.h"

#include <iostream>
#include <vector>
#include <cstdint>
#include <string>

template<typename K, typename V> 
using Map = btree::btree_map<K, V>;

namespace cy {

#define OP_ADD 0
#define OP_DEL 1

#define TYPE_S 0
#define TYPE_M 1
#define TYPE_L 2
#define TYPE_S_MAX 2
#define TYPE_M_MAX 8
#define TYPE_L_MAX 256

#define TYPE_X 5
#define TYPE_X_DEPTH 9999

    size_t NumberOfGrows = 0;

    struct Record_t {
        size_t OpIdx;
        uint8_t OpType; // 0-Add, 1-Delete

        Record_t() {}
        Record_t(size_t idx, uint8_t t) : OpIdx(idx), OpType(t) {}
    };

    struct TrieNode_t {
        // TODO Refactor this to use only 1 array
        std::vector<TrieNode_t*> Children;
        std::vector<uint8_t> ChildrenIndex;
        
        // TODO Optimization
        // TODO Create a custom String class that does not copy the contents of the char* for each key
        Map< std::string, TrieNode_t*> ChildrenMap;

        std::vector<Record_t> Records; // allow parallel batched queries

        uint8_t Type;

        TrieNode_t() { init(TYPE_S); }
        TrieNode_t(uint8_t t) { init(t); }

        inline void init(uint8_t t) {
            switch (t) {
                case TYPE_S:
                    Type = TYPE_S;
                    Children.reserve(TYPE_S_MAX);    
                    ChildrenIndex.reserve(TYPE_S_MAX);    
                    break;
                case TYPE_L:
                    Type = TYPE_L;
                    Children.resize(TYPE_L_MAX, nullptr);
                    break;
                case TYPE_X:
                    Type = TYPE_X;
                    break;
            }
        }

        ///////////// Add / Remove ////////////
       
        TrieNode_t* _growWith(const uint8_t cb) {
            NumberOfGrows++;
            switch(Type) {
                case TYPE_S:
                {
                    Type = TYPE_L;

                    const auto oldChildren(Children);
                    Children.resize(0); Children.reserve(TYPE_L_MAX); Children.resize(TYPE_L_MAX, nullptr);
                    for (size_t i=0, sz=oldChildren.size(); i<sz; i++) {
                        Children[ChildrenIndex[i]] = oldChildren[i];
                    }
                    std::vector<uint8_t> empty;
                    ChildrenIndex.swap(empty);

                    TrieNode_t *newNode = new TrieNode_t();
                    Children[cb] = newNode;
                    
                    return newNode;
                }
            }
            std::cerr << "Aborting." << std::endl;
            abort();
            return nullptr;
        }

        // @param s The whole ngram
        TrieNode_t* AddString(const std::string& s) {
            const size_t bsz = s.size();
            const uint8_t* bs = reinterpret_cast<const uint8_t*>(s.data());

            TrieNode_t *cNode = this;
            for (size_t bidx = 0; bidx < bsz; bidx++) {
                const uint8_t cb = bs[bidx];

                switch(cNode->Type) {
                case TYPE_S:
                    {
                        size_t cidx;
                        auto& childrenIndex = cNode->ChildrenIndex;
                        const size_t csz = childrenIndex.size();
                        for (cidx = 0; cidx<csz; cidx++) {
                            if (childrenIndex[cidx] == cb) {
                                break;
                            }
                        }
                        if (cidx >= csz) {
                            if (csz == TYPE_S_MAX) {
                                cNode = cNode->_growWith(cb);
                            } else {
                                if (bidx >= TYPE_X_DEPTH) {
                                    cNode->Children.push_back(new TrieNode_t(TYPE_X));
                                } else {
                                    cNode->Children.push_back(new TrieNode_t());
                                }
                                childrenIndex.push_back(cb);
                                cNode = cNode->Children.back();
                            }
                        } else {
                            cNode = cNode->Children[cidx];
                        }

                        break;
                    }
                case TYPE_L:
                    {
                        if (!cNode->Children[cb]) {
                            if (bidx >= TYPE_X_DEPTH) {
                                cNode->Children[cb] = new TrieNode_t(TYPE_X);
                            } else {
                                cNode->Children[cb] = new TrieNode_t();
                            }
                        }
                        cNode = cNode->Children[cb];
                        break;
                    }
                case TYPE_X:
                    {
                        const std::string key(s.substr(bidx));
                        auto it = cNode->ChildrenMap.find(key);
                        if (it == cNode->ChildrenMap.end()) {
                            it = cNode->ChildrenMap.insert(it, {std::move(key), new TrieNode_t(TYPE_X)});
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
        TrieNode_t* FindString(const std::string& s) {
            const size_t bsz = s.size();
            const uint8_t* bs = reinterpret_cast<const uint8_t*>(s.data());

            TrieNode_t *cNode = this;
            for (size_t bidx = 0; bidx < bsz; bidx++) {
                const uint8_t cb = bs[bidx];

                switch(cNode->Type) {
                case TYPE_S:
                    {
                        size_t cidx;
                        const auto& childrenIndex = cNode->ChildrenIndex;
                        const size_t csz = childrenIndex.size();
                        for (cidx = 0; cidx<csz; cidx++) {
                            if (childrenIndex[cidx] == cb) {
                                break;
                            }
                        }
                        if (cidx >= csz) {
                            return nullptr;
                        }
                        
                        cNode = cNode->Children[cidx];
                        break;
                    }
                case TYPE_L:
                    {
                        if (!cNode->Children[cb]) {
                            return nullptr;
                        }
                        cNode = cNode->Children[cb];
                        break;
                    }
                case TYPE_X:
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


        // @param s The whole doc prefix that we need to find ALL NGRAMS matching
        static std::vector<size_t> FindAll(TrieNode_t* cNode, const char* s, const size_t docSize, int opIdx) {
            const size_t bsz = docSize;
            const uint8_t* bs = reinterpret_cast<const uint8_t*>(s);

            // Holds the endPos for each valid ngram found in the given doc
            std::vector<size_t> results;

            for (size_t bidx = 0; bidx < bsz; bidx++) {
                const uint8_t cb = bs[bidx];

                switch(cNode->Type) {
                case TYPE_S:
                    {
                        size_t cidx;
                        const auto& childrenIndex = cNode->ChildrenIndex;
                        const size_t csz = childrenIndex.size();
                        for (cidx = 0; cidx<csz; cidx++) {
                            if (childrenIndex[cidx] == cb) {
                                break;
                            }
                        }
                        if (cidx >= csz) {
                            return std::move(results);
                        }
                        
                        cNode = cNode->Children[cidx];
                        break;
                    }
                case TYPE_L:
                    {
                        if (!cNode->Children[cb]) {
                            return std::move(results);
                        }
                        cNode = cNode->Children[cb];
                        break;
                    }
                case TYPE_X:
                    {
                        const std::string key(s+bidx, s+bidx+bsz);
                        auto it = cNode->ChildrenMap.find(key);
                        if (it == cNode->ChildrenMap.end()) {
                            return std::move(results);
                        }
                        cNode = it->second; bidx = bsz;
                        // TODO CALL THE REAL METHOD THAT FINDS THE RESULTS
                        abort();
                        return std::move(results);
                        break;
                    }
                default:
                    abort();
                }

                // at the end of each word check if the ngram so far is a valid result
                if (bs[bidx+1] == ' ' && cNode->IsValid(opIdx)) {
                    results.push_back(bidx+1);
                }
            }
            
            // We are here it means the whole doc matched the ngram ending at cNode
            if (cNode && cNode->IsValid(opIdx)) {
                results.emplace_back(bsz);
            }

            return std::move(results);
        }

        ///////////// Updates related /////////////

        inline void MarkAdd(size_t opIdx) { Records.emplace_back(opIdx, OP_ADD); }
        inline void MarkDel(size_t opIdx) { Records.emplace_back(opIdx, OP_DEL); }
        inline bool IsValid(size_t opIdx) const {
            if (Records.empty()) { return false; }
            const size_t rsz = Records.size();
            
            if (Records[rsz-1].OpIdx < opIdx) {
                return Records[rsz-1].OpType == OP_ADD;
            }
            for (size_t ridx=rsz-1; ridx>0; ridx--) {
                if (Records[ridx-1].OpIdx < opIdx) {
                    return Records[ridx-1].OpType == OP_ADD;
                }
            }
            return false;
        }

    };

    struct TrieRoot_t {
        TrieNode_t Root;

        ~TrieRoot_t() {
            std::cerr << "upgrades::" << NumberOfGrows << std::endl;
        }
    };

};

#endif
