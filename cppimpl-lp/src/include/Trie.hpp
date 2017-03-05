#ifndef __CY_TRIE__
#define __CY_TRIE__

#pragma once

#include "Timer.hpp"
#include <iostream>
#include <vector>
#include <cstdint>
#include <string>

namespace cy {

#define OP_ADD 0
#define OP_DEL 1

#define TYPE_S 0
#define TYPE_M 1
#define TYPE_L 2
#define TYPE_S_MAX 2
#define TYPE_M_MAX 8
#define TYPE_L_MAX 256

    struct Record_t {
        size_t OpIdx;
        uint8_t OpType; // 0-Add, 1-Delete

        Record_t() {}
        Record_t(size_t idx, uint8_t t) : OpIdx(idx), OpType(t) {}
    };

    struct TrieNode_t {
        std::vector<TrieNode_t*> Children;
        std::vector<uint8_t> ChildrenIndex;
        
        std::vector<Record_t> Records;

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
            }
        }

        ///////////// Add / Remove ////////////

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
                            // TODO Upgrade the node type if necessary
                            cNode->Children.push_back(new TrieNode_t());
                            childrenIndex.push_back(cb);
                            cNode = cNode->Children.back();
                        } else {
                            cNode = cNode->Children[cidx];
                        }

                        break;
                    }
                case TYPE_L:
                    {
                        if (!cNode->Children[cb]) {
                            cNode->Children[cb] = new TrieNode_t();
                        }
                        cNode = cNode->Children[cb];
                        break;
                    }
                default:
                    abort();
                }
            }

            return cNode;
        }
         
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
                default:
                    abort();
                }
            }

            return cNode;
        }   

        ///////////// Updates related /////////////

        inline void MarkAdd(size_t opIdx) { Records.emplace_back(opIdx, OP_ADD); }
        inline void MarkDel(size_t opIdx) { Records.emplace_back(opIdx, OP_DEL); }
        bool IsValid(size_t opIdx) const {
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
    };

};

#endif
