package main

import ()

type TrieNode struct {
	ChildrenKeys map[string]struct{}
	Children     map[byte][]TrieNode
	Word         string
	Valid        bool
}

type TrieRoot struct {
	Root TrieNode
}

func buildTrieNode(word string) TrieNode {
	return TrieNode{
		ChildrenKeys: make(map[string]struct{}),
		Children:     make(map[byte][]TrieNode),
		Word:         word,
		Valid:        false,
	}
}

func buildTrie() TrieRoot {
	return TrieRoot{buildTrieNode("_|_")}
}

func (tn *TrieNode) AddWord(word string) *TrieNode {

	nodes, ok := tn.Children[word[0]]
	if !ok {
		nodes = make([]TrieNode, 0, 4) // 4 words for starters
	} else {
		if _, ok := tn.ChildrenKeys[word]; ok {
			return tn.FindWord(word)
		}
	}

	tn.ChildrenKeys[word] = Empty{}

	nodes = append(nodes, buildTrieNode(word))
	tn.Children[word[0]] = nodes
	return &nodes[len(nodes)-1]
}

func (tn *TrieNode) FindWord(word string) *TrieNode {
	nodes, ok := tn.Children[word[0]]
	if !ok {
		return nil
	}

	for i, w := range nodes {
		if w.Word == word {
			return &nodes[i]
		}
	}

	return nil // should never come here
}
