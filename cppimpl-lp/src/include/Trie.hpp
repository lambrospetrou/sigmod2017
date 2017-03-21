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

#include <emmintrin.h>

//#define LPDEBUG 1

//#define USE_TYPE_X

namespace cy {
namespace trie {

    template<typename K, typename V> 
        using Map = btree::btree_map<K, V>;

    enum class OpType : uint8_t { ADD = 0, DEL = 1 };
    enum class NodeType : uint8_t { S = 0, M = 1, L = 2, X = 3 };

    constexpr size_t TYPE_S_MAX = 2;
    constexpr size_t TYPE_M_MAX = 16;
    constexpr size_t TYPE_L_MAX = 256;
    constexpr size_t TYPE_X_DEPTH = 24;

    constexpr size_t MEMORY_POOL_BLOCK_SIZE_S = 1<<25;
    constexpr size_t MEMORY_POOL_BLOCK_SIZE_M = 1<<20;
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
    
    //////////////////////////////////////////


    /////////////////////////////////////////

    struct RecordHistory {
        /*
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
        */

        OpType state{OpType::DEL};
        inline void MarkAdd(size_t opIdx) { state = OpType::ADD; }
        inline void MarkDel(size_t opIdx) { state = OpType::DEL; }
        inline bool IsValid(size_t opIdx) const { return state == OpType::ADD; }
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
            uint8_t ChildrenIndex[sizeof(uint8_t) * SIZE + sizeof(NodePtr*)*SIZE];
            uint8_t Size;

            DataS() : Size(0) {}
            
            inline NodePtr* Children() { return reinterpret_cast<NodePtr*>(ChildrenIndex + sizeof(uint8_t) * SIZE); }
        };


    // Aligned    : 128::256::2112::64
    // Non-Aligned: 96::200::2088::48

    struct TrieNodeS_t {
        const NodeType Type = NodeType::S;
        //RecordHistory State;
        bool Valid;
	std::string Suffix;
        DataS<TYPE_S_MAX> DtS;    
    };
    struct TrieNodeM_t {
        const NodeType Type = NodeType::M;
        //RecordHistory State;
        bool Valid;
        // 40 bytes so far. To be 16-bit aligned for SIMD we need to pad some bytes
        uint8_t padding[8];

        DataS<TYPE_M_MAX> DtM;
    } ALIGNED_16;
    struct TrieNodeL_t {
        const NodeType Type = NodeType::L;
        //RecordHistory State;

        bool Valid;
        std::string Suffix;

        struct DataL {
            NodePtr Children[256];
        } DtL;
    };
    struct TrieNodeX_t {
        const NodeType Type = NodeType::X;
        RecordHistory State; 
        
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
    static inline NodePtr _newTrieNode(MemoryPool_t*mem, size_t depth = 0) {
#ifdef USE_TYPE_X
        if (depth < TYPE_X_DEPTH) {
            return _newTrieNodeS(mem);
        }
        return _newTrieNodeX(mem);
#else
        return _newTrieNodeL(mem);
#endif
    }

       
    ////////////////////////////


    inline static NodePtr _growTypeSWith(MemoryPool_t *mem, TrieNodeS_t *cNode, NodePtr parent, const uint8_t pb, const uint8_t cb) {
        auto newNode = _newTrieNodeM(mem).M;
        
        //newNode->State = std::move(cNode->State);
        newNode->Valid = cNode->Valid;
        newNode->DtM.Size = TYPE_S_MAX+1;

        for (size_t cidx=0; cidx<TYPE_S_MAX; ++cidx) {
            newNode->DtM.Children()[cidx] = cNode->DtS.Children()[cidx];
            newNode->DtM.ChildrenIndex[cidx] = cNode->DtS.ChildrenIndex[cidx];
        }

        auto childNode = _newTrieNode(mem);
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

    inline static NodePtr _growTypeSToLWith(MemoryPool_t *mem, TrieNodeS_t *cNode, NodePtr parent, const uint8_t pb, const uint8_t cb) {
       auto newNode = _newTrieNodeL(mem).L;
        
        //newNode->State = std::move(cNode->State);
        newNode->Valid = cNode->Valid;
      

        for (size_t cidx=0; cidx<TYPE_S_MAX; ++cidx) {
	     newNode->DtL.Children[cNode->DtS.ChildrenIndex[cidx]] =  cNode->DtS.Children()[cidx];
        }

        auto childNode = _newTrieNodeS(mem);
        newNode->DtL.Children[cb] = childNode;
        
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



    inline static NodePtr _growTypeMWith(MemoryPool_t *mem, TrieNodeM_t *cNode, NodePtr parent, const uint8_t pb, const uint8_t cb) {
        auto newNode = _newTrieNodeL(mem).L;
        
        //newNode->State = std::move(cNode->State);
        newNode->Valid = cNode->Valid;
        for (size_t cidx=0; cidx<TYPE_M_MAX; ++cidx) {
            newNode->DtL.Children[cNode->DtM.ChildrenIndex[cidx]] = cNode->DtM.Children()[cidx];
        }
        auto childNode = _newTrieNode(mem);
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


    static inline NodePtr _doAddStringL(MemoryPool_t *mem, TrieNodeL_t *cNode, const uint8_t*bs, const size_t bsz, const size_t bidx, bool *done) {
        const uint8_t cb = bs[bidx];


////std::cerr<<"TYPE l::"<<std::endl;
	if (cNode->Suffix.empty()) { // We just need to check children
            NodePtr nextNode = cNode->DtL.Children[cb];
            if (!nextNode) {
                nextNode = _newTrieNodeS(mem);
                if (bidx+1 < bsz) { // this is NOT the last byte so add the remaining as suffix
                    nextNode.S->Suffix = std::move(std::string(bs+bidx+1, bs+bsz));
                    *done = true;
                }
                cNode->DtL.Children[cb] = nextNode;
            }
            // If this is the last byte of the ngram mark its node as valid
            if (bidx+1 == bsz) {
		if(nextNode.L->Type == NodeType::L)
		 nextNode.L->Valid = true;
		else
		 nextNode.S->Valid = true;
		
               
                *done = true;
            }
	
            return nextNode;
	 } 





        
        // We are at a LEAF with suffix
        const auto& suffix = cNode->Suffix;
        const size_t sufsz = suffix.size();
        const uint8_t *sufbs = reinterpret_cast<const uint8_t*>(cNode->Suffix.data());
        size_t common = 0;
	////std::cerr<<"we are in L-LEAF::suffix::"<<suffix<<"::WHATS LEFT::"<<std::string(bs+bidx, bs+bsz)<<std::endl;
        for (;common < bsz-bidx && common < sufsz && sufbs[common] == bs[bidx+common];) { ++common; }
        
        if (common == sufsz) { // the new ngram matched the whole existing suffix
            if (common == bsz-bidx) { // we are already at the proper node - don't do anything
                *done = true;
                return cNode;
            }
            // there is some part of the new ngram to be added so we need to create the nodes
            // to cover the common bytes and then we will add as suffix the remaining part of the new ngram
	    //todo:convert the LARGE TYPE LEAF to an S TYPE ...WE NEED TO PASS PARENT FOR THAT
	
	    ////std::cerr<<"we are in L-LEAF::common"<<common<<std::endl;

	

	    NodePtr nextNode = cNode;
	    if(common!=0){						
	    cNode->DtL.Children[sufbs[0]] = _newTrieNodeS(mem);
            nextNode = cNode->DtL.Children[sufbs[0]];
		}
	    for (size_t sidx=1; sidx<common; ++sidx) {
                
		nextNode.S->DtS.Children()[nextNode.S->DtS.Size] = _newTrieNodeS(mem);
                nextNode.S->DtS.ChildrenIndex[nextNode.S->DtS.Size] = sufbs[sidx];
								
                nextNode = nextNode.S->DtS.Children()[nextNode.S->DtS.Size++];

		//nextNode.L->DtL.Children[sufbs[sidx]] = _newTrieNode(mem, bidx);
                //nextNode = nextNode.L->DtL.Children[sufbs[sidx]];
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
          	
	  NodePtr nextNode = cNode;
	   if(common!=0){	
	  cNode->DtL.Children[sufbs[0]] = _newTrieNodeS(mem);
          nextNode = cNode->DtL.Children[sufbs[0]];
		}
        for (size_t sidx=1; sidx<common; ++sidx) {
	   size_t size = nextNode.S->DtS.Size;
           nextNode.S->DtS.Children()[size] = _newTrieNodeS(mem);
           nextNode.S->DtS.ChildrenIndex[size] = sufbs[sidx];
	   nextNode.S->DtS.Size++;								
           nextNode = nextNode.S->DtS.Children()[size];
        }
        // add the remaining of the existing ngram
	auto newNode = _newTrieNodeS(mem);;
	nextNode.S->DtS.Children()[nextNode.S->DtS.Size] = newNode;
        nextNode.S->DtS.ChildrenIndex[nextNode.S->DtS.Size++] = sufbs[common];
	
        if (common+1 == sufsz) {
            // there was only 1 byte remaining and it was added through a new node.
            newNode.S->Valid = true;
        } else {
            newNode.S->Suffix = std::move(suffix.substr(common+1));
        }

        // add the remaining of the new ngram
        if (bidx+common < bsz) {

	    newNode = _newTrieNodeS(mem);
	    nextNode.S->DtS.Children()[nextNode.S->DtS.Size] = newNode;
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


     static inline NodePtr _doAddStringS(MemoryPool_t *mem, TrieNodeS_t *cNode,NodePtr parent, const uint8_t*bs, const size_t bsz, const size_t bidx, bool *done) {
        const uint8_t cb = bs[bidx];
	
	 		 
     
	const auto childrenIndex = cNode->DtS.ChildrenIndex;
        const size_t csz = cNode->DtS.Size;
	size_t cidx = 0;
        if (cNode->Suffix.empty()) { // We just need to check children
	    ////std::cerr<<"we are in S::checking children"<<std::endl;
            NodePtr nextNode; //= childrenIndex[csz];    //cNode->DtL.Children[cb];
		bool childFound = true;
             for (;;) {
                            if (cidx >= csz) {
                                if (csz == TYPE_S_MAX) {
				 // //std::cerr<<"we are in S::not found growing"<<std::endl;
                                   nextNode = _growTypeSToLWith(mem, cNode, parent, bs[bidx-1], cb);  //we try to add cb but we exceed the max children so we create an L and we point to an S (the return)
                                } else {
					////std::cerr<<"we are in S::not found adding it::"<<cb<<std::endl;
                                    cNode->DtS.Children()[cNode->DtS.Size++] = _newTrieNodeS(mem);
                                    childrenIndex[csz] = cb;
                                    nextNode = cNode->DtS.Children()[csz];
                                }
				childFound = false;
                                break;
                            }
                            if (childrenIndex[cidx] == cb) {
                                nextNode = cNode->DtS.Children()[cidx];
                                break;
                            }
                            cidx++;
                }
		if (!childFound) {
                //nextNode = _newTrieNode(mem, bidx);
                if (bidx+1 < bsz) { // this is NOT the last byte so add the remaining as suffix
		    ////std::cerr<<"we are in S::not found adding suffix::"<<std::string(bs+bidx+1, bs+bsz)<<std::endl;
                    nextNode.S->Suffix = std::move(std::string(bs+bidx+1, bs+bsz));
                    *done = true;
		    
                }
               // cNode->DtL.Children[cb] = nextNode;
            }
            // If this is the last byte of the ngram mark its node as valid
            if (bidx+1 == bsz) {
		////std::cerr<<"we are in S::valid true"<<std::endl;
                nextNode.S->Valid = true;
                *done = true;
            }
	    ////std::cerr<<"returningS"<<std::endl;
            return nextNode;
        } 

        // We are at a LEAF with suffix
        const auto& suffix = cNode->Suffix;
        const size_t sufsz = suffix.size();
        const uint8_t *sufbs = reinterpret_cast<const uint8_t*>(cNode->Suffix.data());
	////std::cerr<<"we are in S-LEAF::suffix"<<suffix<<"::WHATS LEFT::"<<std::string(bs+bidx, bs+bsz)<<std::endl;

        size_t common = 0;
        for (;common < bsz-bidx && common < sufsz && sufbs[common] == bs[bidx+common];) { ++common; }
        
        if (common == sufsz) { // the new ngram matched the whole existing suffix
            if (common == bsz-bidx) { // we are already at the proper node - don't do anything
                *done = true;
                return cNode;
            }
            // there is some part of the new ngram to be added so we need to create the nodes
            // to cover the common bytes and then we will add as suffix the remaining part of the new ngram
            NodePtr nextNode = cNode;
            
	    //todo:convert the LARGE TYPE LEAF to an S TYPE ...WE NEED TO PASS PARENT FOR THAT

	   if(common !=0){
	    cNode->DtS.Children()[cNode->DtS.Size] = _newTrieNodeS(mem);
            cNode->DtS.ChildrenIndex[cNode->DtS.Size] = sufbs[0];						
            nextNode = cNode->DtS.Children()[cNode->DtS.Size++];
	 }
	    for (size_t sidx=1; sidx<common; ++sidx) {
		nextNode.S->DtS.Children()[nextNode.S->DtS.Size] = _newTrieNodeS(mem);
                nextNode.S->DtS.ChildrenIndex[nextNode.S->DtS.Size] = sufbs[sidx];					
                nextNode = nextNode.S->DtS.Children()[nextNode.S->DtS.Size++];
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
        NodePtr nextNode = cNode;
	if(common !=0){
	cNode->DtS.Children()[cNode->DtS.Size] = _newTrieNodeS(mem);
        cNode->DtS.ChildrenIndex[cNode->DtS.Size] = sufbs[0];						
        nextNode = cNode->DtS.Children()[cNode->DtS.Size++];
	}
        for (size_t sidx=1; sidx<common; ++sidx) {
           nextNode.S->DtS.Children()[nextNode.S->DtS.Size] = _newTrieNodeS(mem);
           nextNode.S->DtS.ChildrenIndex[nextNode.S->DtS.Size] = sufbs[sidx];
								
           nextNode = nextNode.S->DtS.Children()[nextNode.S->DtS.Size++];
        }
        // add the remaining of the existing ngram

	nextNode.S->DtS.Children()[nextNode.S->DtS.Size] = _newTrieNodeS(mem);
        nextNode.S->DtS.ChildrenIndex[nextNode.S->DtS.Size] = sufbs[common];						
        NodePtr newNode = nextNode.S->DtS.Children()[nextNode.S->DtS.Size++];
        if (common+1 == sufsz) {
            // there was only 1 byte remaining and it was added through a new node.
            newNode.S->Valid = true;
        } else {
            newNode.S->Suffix = std::move(suffix.substr(common+1));
        }

        // add the remaining of the new ngram
        if (bidx+common < bsz) {
	    nextNode.S->DtS.Children()[nextNode.S->DtS.Size] = _newTrieNodeS(mem);
            nextNode.S->DtS.ChildrenIndex[nextNode.S->DtS.Size] = bs[bidx+common];
            newNode = nextNode.S->DtS.Children()[nextNode.S->DtS.Size++];
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
    static NodePtr AddString(MemoryPool_t *mem, NodePtr cNode, const std::string& s) {
        const size_t bsz = s.size();
	////std::cerr<<"Adding::"<<s<<std::endl;
        const uint8_t* bs = reinterpret_cast<const uint8_t*>(s.data());
        bool done = false;
        NodePtr parent = (TrieNodeS_t*)nullptr;
        NodePtr previous = (TrieNodeS_t*)nullptr;
        for (size_t bidx = 0; bidx < bsz; bidx++) {
            const uint8_t cb = bs[bidx];

            previous = cNode;

            switch(cNode.L->Type) {
                case NodeType::S:
                    {

			
			////std::cerr<<"we are in S::"<<s.substr(bidx)<<std::endl;
                        cNode = _doAddStringS(mem, cNode.S, parent, bs, bsz, bidx, &done);
                        break;
                    }
                case NodeType::M:
                    {
                        const auto mNode = cNode.M;
                        const size_t csz = mNode->DtM.Size;
                        
                        auto key =_mm_set1_epi8(cb);
                        auto cmp =_mm_cmpeq_epi8(key, *(__m128i*)mNode->DtM.ChildrenIndex);
                        auto mask=(1<<csz)-1;
                        auto bitfield=_mm_movemask_epi8(cmp)&mask;
                         
#ifdef LPDEBUG
                        //std::cerr << (void*)mNode << ":" << is_aligned(mNode, 16) << std::endl;
                        
                        auto& childrenIndex = mNode->DtM.ChildrenIndex;
                        size_t tcidx = 0;
                        for (tcidx = 0; tcidx<csz; tcidx++) {
                            if (childrenIndex[tcidx] == cb) {
                                break;
                            }
                        }
                        if (tcidx>=csz && bitfield > 0) {
                            //std::cerr << (void*)mNode << ":" << is_aligned(mNode, 16) << std::endl;
                            //std::cerr << (void*)mNode->DtM.ChildrenIndex << ":" << is_aligned(mNode->DtM.ChildrenIndex, 16) << std::endl;
                            //std::cerr << mask << "::" << bitfield << "::" << csz << "::"<< "::" << tcidx << std::endl;
                            abort();
                        }
#endif

                        if (!bitfield) {
                            if (csz == TYPE_M_MAX) {
                                cNode = _growTypeMWith(mem, mNode, parent, bs[bidx-1], cb);
                            } else {
                                mNode->DtM.Children()[mNode->DtM.Size++] = _newTrieNode(mem, bidx);
                                mNode->DtM.ChildrenIndex[csz] = cb;
                                cNode = mNode->DtM.Children()[csz];
                            }
                        } else {
                            cNode = mNode->DtM.Children()[__builtin_ctz(bitfield)];
                        }

                        break;
                    }
                case NodeType::L:
                    {
                        cNode = _doAddStringL(mem, cNode.L, bs, bsz, bidx, &done);
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
                        cNode = it->second; bidx = bsz;
                        break;
                        abort();
                    }
                default:
                    abort();
            }
            if (done) { break; }

            parent = previous;
        }

        return cNode;
    }

    static inline NodePtr _doDelStringL(TrieNodeL_t *cNode, const uint8_t*bs, const size_t bsz, const size_t bidx, bool *done) {
        const uint8_t cb = bs[bidx];
        
        if (cNode->Suffix.empty()) { // NOT LEAF
            NodePtr nextNode = cNode->DtL.Children[cb];
            if (!nextNode) {
                *done = true;
                return nullptr;
            }
            if (bidx+1 == bsz) {
                nextNode.L->Valid = false; // make the delete
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

    static inline NodePtr _doDelStringS(TrieNodeS_t *cNode, const uint8_t*bs, const size_t bsz, const size_t bidx, bool *done) {
        const uint8_t cb = bs[bidx];
        


        if (cNode->Suffix.empty()) { // NOT LEAF
            NodePtr nextNode;// = cNode->DtL.Children[cb];
	    const auto childrenIndex = cNode->DtS.ChildrenIndex;
            const size_t csz = cNode->DtS.Size;
	    size_t cidx = 0;
	    bool found = false;	
	    for (;;) {
		  if (cidx >= csz) {
		      found = false;
		  break;
		  }
	    	  if (childrenIndex[cidx] == cb) {
		      nextNode = cNode->DtS.Children()[cidx];
                      found=true;
                  break;
                  }
		cidx++;
            }


            if (!found) {
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
            const uint8_t cb = bs[bidx];

            switch(cNode.L->Type) {
                case NodeType::S:
                    {
                        cNode = _doDelStringS(cNode.S, bs, bsz, bidx, &done);
                        break;    
                    }
                case NodeType::M:
                    {
                        const auto mNode = cNode.M;
                        const size_t csz = mNode->DtM.Size;

                        auto key =_mm_set1_epi8(cb);
                        auto cmp =_mm_cmpeq_epi8(key, *(__m128i*)mNode->DtM.ChildrenIndex);
                        auto mask=(1<<csz)-1;
                        auto bitfield=_mm_movemask_epi8(cmp)&mask;
                        
                        if (!bitfield) {
                            return;
                        }
                        
#ifdef LPDEBUG
                        auto childrenIndex = mNode->DtM.ChildrenIndex;
                        size_t cidx = 0;
                        for (cidx = 0; cidx<csz; cidx++) {
                            if (childrenIndex[cidx] == cb) {
                                break;
                            }
                        }
                      
                        if (cidx < csz && bitfield <= 0) {
                            //std::cerr << (void*)mNode << ":" << is_aligned(mNode, 16) << std::endl;
                            //std::cerr << (void*)mNode->DtM.ChildrenIndex << ":" << is_aligned(mNode->DtM.ChildrenIndex, 16) << std::endl;
                            
                            //std::cerr << mask << "::" << bitfield << "::" << csz << "::" << cidx << std::endl;
                            abort();
                        }
#endif

                        cNode = mNode->DtM.Children()[__builtin_ctz(bitfield)];
                        break;    
                    }
                case NodeType::L:
                    {
                        /*
                        if (!cNode.L->DtL.Children[cb]) {
                            return nullptr;
                        }
                        cNode = cNode.L->DtL.Children[cb];
                        */
                        cNode = _doDelStringL(cNode.L, bs, bsz, bidx, &done);
                        break;
                    }
                case NodeType::X:
                    {
                        const auto xNode = cNode.X;
                        const std::string key(s.substr(bidx));
                        const auto it = xNode->ChildrenMap.find(key);
                        if (it == xNode->ChildrenMap.end()) {
                            return;
                        }
                        cNode = it->second; bidx = bsz;
                        break;
                    }
                default:
                    abort();
            }
            if (done) { break; }
        }

        return;
    }

    inline static std::vector<std::pair<size_t, uint64_t>> _findAllTypeX(TrieNodeX_t *cNode, std::vector<std::pair<size_t,uint64_t>>& results, const char*s, size_t bidx, const size_t bsz, int opIdx) {

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
            ////std::cerr << "No Match." << std::endl;
            return std::move(results);
        }

        for (auto it=lb, end=children.end(); it != end; ++it) {
            ////std::cerr << "Matching...." << it->first << std::endl;
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

    // @param s The whole doc prefix that we need to find ALL NGRAMS matching
    static std::vector<std::pair<size_t, uint64_t>> FindAll(NodePtr cNode, const char *s, const size_t docSize, int opIdx) {
        const size_t bsz = docSize;
        const uint8_t* bs = reinterpret_cast<const uint8_t*>(s);
	
        // Holds the endPos for each valid ngram found in the given doc and the identifier for the ngram (pointer for now)
        std::vector<std::pair<size_t, uint64_t>> results;

        for (size_t bidx = 0; bidx < bsz; bidx++) {
            const uint8_t cb = bs[bidx];

            switch(cNode.L->Type) {
                case NodeType::S:
                    {
			 if (cNode.S->Suffix.empty()) {
                            const auto sNode = cNode.S;
                            const auto childrenIndex = sNode->DtS.ChildrenIndex;
                            const size_t csz = sNode->DtS.Size;
                            size_t cidx = 0;
                            for (;;) {
                            	if (cidx >= csz) { return std::move(results); }
                            	if (childrenIndex[cidx] == cb) {
                                cNode = sNode->DtS.Children()[cidx];
                                break;
                            	}
                            cidx++;
                           }

                        } else {
			   
                            // the doc has to match the whole ngram suffix
                            auto suffix = reinterpret_cast<const uint8_t*>(cNode.S->Suffix.data());
                            const auto sufsz = cNode.S->Suffix.size();
                            if (sufsz > bsz-bidx) { return std::move(results); }
                            for (size_t sufidx=0; sufidx<sufsz; sufidx++) {
                                if (suffix[sufidx] != bs[bidx+sufidx]) { return std::move(results); }
                            }
                            bidx += sufsz;
                            if (bidx >= bsz || bs[bidx] == ' ') {
                                results.emplace_back(bidx, (uint64_t)suffix);
                            }
                            return std::move(results);
                        }
                        break;
                    }
                case NodeType::M:
                    {
                        const auto mNode = cNode.M;
                        const size_t csz = mNode->DtM.Size;
                        
                        auto key =_mm_set1_epi8(cb);
                        auto cmp =_mm_cmpeq_epi8(key, *(__m128i*)mNode->DtM.ChildrenIndex);
                        auto mask=(1<<csz)-1;
                        auto bitfield=_mm_movemask_epi8(cmp)&mask;
                        if (!bitfield) {
                            return std::move(results);
                        }
                        cNode = mNode->DtM.Children()[__builtin_ctz(bitfield)];
                        break;
                    }
                case NodeType::L:
                    {
                        if (cNode.L->Suffix.empty()) {
                            if (!cNode.L->DtL.Children[cb]) {
                                return std::move(results);
                            }
                            cNode = cNode.L->DtL.Children[cb];
                        } else {
                            // the doc has to match the whole ngram suffix
                            auto suffix = reinterpret_cast<const uint8_t*>(cNode.L->Suffix.data());
                            const auto sufsz = cNode.L->Suffix.size();
                            if (sufsz > bsz-bidx) { return std::move(results); }
                            for (size_t sufidx=0; sufidx<sufsz; sufidx++) {
                                if (suffix[sufidx] != bs[bidx+sufidx]) { return std::move(results); }
                            }
                            bidx += sufsz;
                            if (bidx >= bsz || bs[bidx] == ' ') {
                                results.emplace_back(bidx, (uint64_t)suffix);
                            }
                            return std::move(results);
                        }
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
	    if(cNode.L->Type == NodeType::S)
	    {
		if (bs[bidx+1] == ' ' && cNode.S->Valid)  {
                results.emplace_back(bidx+1, (uint64_t)cNode.S);
            	}

	    }
	    else
	    { 
		if (bs[bidx+1] == ' ' && cNode.L->Valid)  {
                results.emplace_back(bidx+1, (uint64_t)cNode.L);
            	}
	    }

            
        }

        // For Types S,M,L
        // We are here it means the whole doc matched the ngram ending at cNode
        //if (cNode && cNode.L->State.IsValid(opIdx)) {
	if(cNode.L->Type == NodeType::S)
	{
	if (cNode && cNode.S->Valid) {
            results.emplace_back(bsz, (uint64_t)cNode.S);
        }

	}


	else{
        if (cNode && cNode.L->Valid  ) {
            results.emplace_back(bsz, (uint64_t)cNode.L);
        }
	}
        return std::move(results);
    }

    
    // @param s The whole ngram
    static NodePtr SearchString(NodePtr cNode, const std::string& s) {
        const size_t bsz = s.size();
        const uint8_t* bs = reinterpret_cast<const uint8_t*>(s.data());

        //std::cerr << "Search: " << s << std::endl;

        for (size_t bidx = 0; bidx < bsz; bidx++) {
            const uint8_t cb = bs[bidx];

            switch(cNode.L->Type) {        
                case NodeType::L:
                    {
                        if (cNode.L->Suffix.empty()) {
                            if (!cNode.L->DtL.Children[cb]) {
                                return nullptr;
                            } 
                            cNode = cNode.L->DtL.Children[cb];
                            if (bidx+1 == bsz) {
                                return cNode.L->Valid ? cNode : nullptr;
                            }
                        } else {
                            size_t suffix_length = cNode.L->Suffix.size();
                            if (suffix_length != bsz - bidx) return nullptr;
 
                            for (size_t i = 0;;) {
                                if (i == suffix_length || bidx == bsz) {
                                    if (!(i == suffix_length && bidx == bsz)) {
                                        return nullptr;
                                    }
                                    return cNode;
                                }                                

                                if (cNode.L->Suffix[i] != bs[bidx]) {
                                    return nullptr;
                                }
                                i++; bidx++;
                            }
                        }
                        break;
                    }
                default:
                    abort();
            }
        }

        return nullptr;
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

    struct TrieRoot_t {
        NodePtr Root;
        MemoryPool_t MemoryPool;

        TrieRoot_t() {
            //std::cerr << sizeof(TrieNodeS_t) << "::" << sizeof(TrieNodeM_t) <<  "::" << sizeof(TrieNodeL_t) <<  "::" << sizeof(TrieNodeX_t) << "::" << sizeof(DataS<2>) << "::" << sizeof(DataS<16>) << "::" << sizeof(RecordHistory) << "::" << sizeof(NodePtr) << std::endl;
            
            //std::cerr << "S" << TYPE_S_MAX << " L" << TYPE_L_MAX << " X" << TYPE_X_DEPTH;
            //std::cerr << " MEM_S" << MEMORY_POOL_BLOCK_SIZE_S;
            //std::cerr << " MEM_M" << MEMORY_POOL_BLOCK_SIZE_M;
            //std::cerr << " MEM_L" << MEMORY_POOL_BLOCK_SIZE_L;
            //std::cerr << " MEM_X" << MEMORY_POOL_BLOCK_SIZE_X;
            //std::cerr << std::endl;

            Root = _newTrieNodeL(&MemoryPool);
            if (!Root) { abort(); }
        }
        ~TrieRoot_t() {
            _takeAnalytics(Root);
            //std::cerr << "growsM::" << GrowsM << " growsL::" << GrowsL << " #nodes::" << NumberOfNodes << std::endl;
            //std::cerr << "MAP::" << xTotal << "::" << xCh0 << "::" << xChMin << "::" << xChMax << "::" << (xChTotal*1.0/xTotal) << std::endl;
        }
    };
    
    inline static void AddNgram(TrieRoot_t *trie, const std::string& s, int opIdx) {
        ////std::cerr << "a::" << s << std::endl;
        auto cNode = cy::trie::AddString(&trie->MemoryPool, trie->Root, s);
        //cNode.L->State.MarkAdd(opIdx);
#ifdef LPDEBUG
        if (cNode != cy::trie::SearchString(trie->Root, s)) {
            //std::cerr << "add or find is wrong!" << std::endl;
            abort();
        }
#endif
        //cy::trie::AddString(&trie->MemoryPool, trie->Root, s);
    }

    inline static void RemoveNgram(TrieRoot_t*trie, const std::string& s, int opIdx) {
        ////std::cerr << "rem::" << s << std::endl;
        //auto cNode = cy::trie::FindString(trie->Root, s);
        //if (cNode) {
        //    cNode.L->State.MarkDel(opIdx);
        //}
        cy::trie::DelString(trie->Root, s);
    }

};
};

#endif
