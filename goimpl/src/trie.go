package main

import (
//"time"
//"fmt"
//"os"
)

const (
	OP_ADD = byte(0)
	OP_DEL = byte(1)
)

type OpRecord struct {
	Op    byte // 0 = Add, 1 = Delete
	OpIdx int
}

type TrieNode struct {
	ChildrenIndex []byte
	ChildrenKeys  []*TrieNode
	Records       []OpRecord
}

type TrieRoot struct {
	Root TrieNode
}

var ChildrenTrie uint64 = 0

func buildTrieNode() *TrieNode {
	ChildrenTrie++
	return &TrieNode{
		ChildrenKeys:  make([]*TrieNode, 0, 2),
		ChildrenIndex: make([]byte, 0, 2),
		Records:       make([]OpRecord, 0, 1),
	}
}

func buildTrie() TrieRoot {
	return TrieRoot{*buildTrieNode()}
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

func (tn *TrieNode) AddString(s string) *TrieNode {
	bs := []byte(s)
	bsz := len(bs)
	cNode := tn
	cidx := 0
	for bidx := 0; bidx < bsz; bidx++ {
		cb := bs[bidx]

		csz := len(cNode.ChildrenIndex)
		for cidx = 0; cidx < csz; cidx++ {
			if cNode.ChildrenIndex[cidx] == cb {
				break
			}
		}

		if cidx >= csz {
			cNode.ChildrenKeys = append(cNode.ChildrenKeys, buildTrieNode())
			cNode.ChildrenIndex = append(cNode.ChildrenIndex, cb)
		}

		cNode = cNode.ChildrenKeys[cidx]
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
	}
	return cNode
}
