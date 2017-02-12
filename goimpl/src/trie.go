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
	ChildrenKeys map[string]int
	Children     map[byte][]TrieNode
	Word         string
	Records      []OpRecord
}

type TrieRoot struct {
	Root TrieNode
}

func buildTrieNode(word string) TrieNode {
	return TrieNode{
		ChildrenKeys: make(map[string]int),
		Children:     make(map[byte][]TrieNode),
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

func (tn *TrieNode) AddWord(word string) (int, *TrieNode) {

	nodes, ok := tn.Children[word[0]]
	if !ok {
		nodes = make([]TrieNode, 0, 4) // 4 words for starters
	} else {
		if nodeIdx, ok := tn.ChildrenKeys[word]; ok {
			return nodeIdx, &tn.Children[word[0]][nodeIdx]
		}
	}

	tn.ChildrenKeys[word] = len(nodes)

	nodes = append(nodes, buildTrieNode(word))
	tn.Children[word[0]] = nodes
	return len(nodes) - 1, &nodes[len(nodes)-1]
}

func (tn *TrieNode) FindWord(word string) (int, *TrieNode) {
	idx, ok := tn.ChildrenKeys[word]
	if !ok {
		return -1, nil
	}
	return idx, &tn.Children[word[0]][idx]
	/*
		nodes, ok := tn.Children[word[0]]
		if !ok {
			return -1, nil
		}

		for i, w := range nodes {
			if w.Word == word {
				return i, &nodes[i]
			}
		}

		return -1, nil // should never come here
	*/
}
