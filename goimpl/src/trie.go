package main

import (
	"time"
	//"fmt"
	//"os"
)

const (
	OP_ADD = byte(0)
	OP_DEL = byte(1)

	// 2^TYPE is the number of available spaces in the node
	TYPE_S = 1
	TYPE_L = 3

	TYPE_S_MAX = 4
	TYPE_L_MAX = 256
)

type OpRecord struct {
	Op    byte // 0 = Add, 1 = Delete
	OpIdx int
}

type TrieNode struct {
	ChildrenIndex []byte
	ChildrenKeys  []*TrieNode
	Records       []OpRecord

	Type uint8
}

type TrieRoot struct {
	Root TrieNode
}

var ChildrenTrie uint64 = 0

func buildTrieNodeL() *TrieNode {
	ChildrenTrie++
	// default start with TYPE_S
	return &TrieNode{
		ChildrenKeys:  make([]*TrieNode, TYPE_L_MAX, TYPE_L_MAX),
		ChildrenIndex: nil,
		Records:       make([]OpRecord, 0, 1),

		Type: TYPE_L,
	}
}
func buildTrieNodeS() *TrieNode {
	ChildrenTrie++
	// default start with TYPE_S
	return &TrieNode{
		ChildrenKeys:  make([]*TrieNode, 0, TYPE_S_MAX),
		ChildrenIndex: make([]byte, 0, TYPE_S_MAX),
		Records:       make([]OpRecord, 0, 0),

		Type: TYPE_S,
	}
}

func buildTrieNode() *TrieNode {
	return buildTrieNodeS()
}

func buildTrie() TrieRoot {
	return TrieRoot{*buildTrieNode()}
}

func (tn *TrieNode) MarkAdd(opIdx int) {
	tn.Records = append(tn.Records, OpRecord{OP_ADD, opIdx})
}
func (tn *TrieNode) MarkDel(opIdx int) {
	tn.Records = append(tn.Records, OpRecord{OP_DEL, opIdx})
}

func (tn *TrieNode) IsValid(opIdx int) bool {
	//defer timeSave(time.Now(), "IsValid()")

	//fmt.Fprintln(os.Stderr, tn)
	sz := len(tn.Records)

	// If not leaf and not a valid ngram yet
	if sz == 0 {
		return false
	}

	// Optimization? Check the upper record before looping
	if tn.Records[sz-1].OpIdx < opIdx {
		return tn.Records[sz-1].Op == OP_ADD
	}

	// Now we have to loop to find the latest status for this query
	for i := sz - 2; i >= 0; i-- {
		if tn.Records[i].OpIdx < opIdx {
			return tn.Records[i].Op == OP_ADD
		}
	}

	return false
}

func (tn *TrieNode) growWith(cb byte) *TrieNode {
	defer timeSave(time.Now(), "g")

	oldKeys := append([]*TrieNode(nil), tn.ChildrenKeys...)
	tn.ChildrenKeys = make([]*TrieNode, TYPE_L_MAX, TYPE_L_MAX)
	for i := 0; i < len(tn.ChildrenIndex); i++ {
		tn.ChildrenKeys[tn.ChildrenIndex[i]] = oldKeys[i]
	}
	tn.ChildrenIndex = nil

	newNode := buildTrieNode()
	tn.ChildrenKeys[cb] = newNode

	tn.Type = TYPE_L

	return newNode
}
func (tn *TrieNode) AddString(s string) *TrieNode {
	bs := []byte(s)
	bsz := len(bs)
	cNode := tn
	cidx := 0
	for bidx := 0; bidx < bsz; bidx++ {
		cb := bs[bidx]

		switch cNode.Type {
		case TYPE_S:
			csz := len(cNode.ChildrenIndex)
			for cidx = 0; cidx < csz; cidx++ {
				if cNode.ChildrenIndex[cidx] == cb {
					break
				}
			}

			if cidx >= csz {
				if csz == TYPE_S_MAX {
					cNode = cNode.growWith(cb)
				} else {
					cNode.ChildrenKeys = append(cNode.ChildrenKeys, buildTrieNode())
					cNode.ChildrenIndex = append(cNode.ChildrenIndex, cb)
					cNode = cNode.ChildrenKeys[cidx]
				}
			} else {
				cNode = cNode.ChildrenKeys[cidx]
			}

			break
		case TYPE_L:
			if cNode.ChildrenKeys[cb] == nil {
				cNode.ChildrenKeys[cb] = buildTrieNode()
			}
			cNode = cNode.ChildrenKeys[cb]
			break
		}
	}
	return cNode
}

func (tn *TrieNode) FindString(s string) *TrieNode {
	//defer timeSave(time.Now(), "FindWord()")
	bs := []byte(s)
	bsz := len(bs)
	cNode := tn
	cidx := 0
	for bidx := 0; bidx < bsz; bidx++ {
		cb := bs[bidx]

		switch cNode.Type {
		case TYPE_S:
			csz := len(cNode.ChildrenIndex)
			for cidx = 0; cidx < csz; cidx++ {
				if cNode.ChildrenIndex[cidx] == cb {
					break
				}
			}

			if cidx >= csz {
				return nil
			}

			cNode = cNode.ChildrenKeys[cidx]
			break
		case TYPE_L:
			if cNode.ChildrenKeys[cb] == nil {
				return nil
			}
			cNode = cNode.ChildrenKeys[cb]
			break
		}
	}
	return cNode
}
