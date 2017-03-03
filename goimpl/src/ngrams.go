package main

import (
//	"fmt"
//	"os"
)

type NgramDB struct {
	Trie TrieRoot
}

type NgramResult struct {
	Term string
	Pos  int
}

func NewNgramDB() NgramDB {
	return NgramDB{
		Trie: buildTrie(),
	}
}

func (ngdb *NgramDB) AddNgram(ngram string, opIdx int) {
	//fmt.Fprintln(os.Stderr, "add", words)
	cNode := ngdb.Trie.Root.AddString(ngram)
	cNode.Records = append(cNode.Records, OpRecord{OP_ADD, opIdx})
}

func (ngdb *NgramDB) RemoveNgram(ngram string, opIdx int) {
	//fmt.Fprintln(os.Stderr, "rem", words)

	cNode := ngdb.Trie.Root.FindString(ngram)
	if cNode != nil {
		cNode.Records = append(cNode.Records, OpRecord{OP_DEL, opIdx})
	}
}

func (ngdb *NgramDB) FindNgrams(partialDoc string, globalStartIdx int, opIdx int) []NgramResult {
	sz := len(partialDoc)

	// skip leading spaces
	start := 0
	for ; start < sz && partialDoc[start] == ' '; start += 1 {
	}

	end := start
	cNode := &ngdb.Trie.Root
	results := make([]NgramResult, 0, 4)
	for start < sz {
		// find the end of the word by skipping spaces first and then letters
		//for end = start; end < sz && partialDoc[end] == ' '; end += 1 {
		//}
		end++ // 1 space
		for ; end < sz && partialDoc[end] != ' '; end += 1 {
		}

		if start == end { // start == end == sz
			break
		}

		//		fmt.Fprintln(os.Stderr, "find ngrams", partialDoc[:end], partialDoc[start:end], len(partialDoc[start:end]))

		cNode = cNode.FindString(partialDoc[start:end])
		if cNode == nil {
			break
		}

		if cNode.IsValid(opIdx) {
			//fmt.Fprintln(os.Stderr, "find ngrams valid", partialDoc[:end])

			results = append(results, NgramResult{
				Term: partialDoc[:end],
				Pos:  globalStartIdx + start,
			})
		}

		start = end
		//for start = end; start < sz && partialDoc[start] == ' '; start += 1 {
		//}
	}

	return results
}
