package main

import (
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
	//ChildrenHash map[uint16]Empty
	ChildrenHash []bool
	ChildrenKeys map[string]*TrieNode
	Word         string
	Records      []OpRecord
}

type TrieRoot struct {
	Root TrieNode
}

func buildTrieNode(word string) TrieNode {
	return TrieNode{
		//ChildrenHash: make(map[uint16]Empty),
		ChildrenHash: make([]bool, 1<<16, 1<<16),
		ChildrenKeys: make(map[string]*TrieNode),
		Word:         word,
		Records:      make([]OpRecord, 0, 2),
	}
}

func buildTrie() TrieRoot {
	return TrieRoot{buildTrieNode("_|_")}
}

func (tn *TrieNode) IsValid(opIdx int) bool {
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

func (tn *TrieNode) AddWord(word string) *TrieNode {
	if n, ok := tn.ChildrenKeys[word]; ok {
		return n
	}
	newNode := buildTrieNode(word)
	tn.ChildrenKeys[word] = &newNode

	if len(word) > 1 {
		tn.ChildrenHash[uint16(word[0])<<8+uint16(word[1])] = true
	} else {
		tn.ChildrenHash[uint16(word[0])] = true
	}

	return &newNode
}

func (tn *TrieNode) FindWord(word string) *TrieNode {
	var wordHash uint16
	if len(word) > 1 {
		wordHash = uint16(word[0])<<8 + uint16(word[1])
	} else {
		wordHash = uint16(word[0])
	}
	//if _, ok := tn.ChildrenHash[wordHash]; !ok {
	if !tn.ChildrenHash[wordHash] {
		return nil
	}

	if n, ok := tn.ChildrenKeys[word]; ok {
		return n
	}
	return nil
}
