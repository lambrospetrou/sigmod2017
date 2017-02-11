package main

import (
	"fmt"
	"os"
	"strings"
)

type NgramDB struct {
	Trie TrieRoot
}

type NgramResult struct {
	Term string
	Pos  int
}

func NewNgramDB() NgramDB {
	fmt.Fprintln(os.Stderr, "new db")
	return NgramDB{
		Trie: buildTrie(),
	}
}

func (ngdb *NgramDB) AddNgram(ngram string) {
	words := strings.Split(strings.Trim(ngram, " "), " ")
	//fmt.Fprintln(os.Stderr, "add", words)

	cNode := &ngdb.Trie.Root
	for _, w := range words {
		cNode = cNode.AddWord(w)
	}
	cNode.Valid = true
}

func (ngdb *NgramDB) RemoveNgram(ngram string) {
	words := strings.Split(strings.Trim(ngram, " "), " ")
	//fmt.Fprintln(os.Stderr, "rem", words)

	cNode := &ngdb.Trie.Root
	for _, w := range words {
		if cNode = cNode.FindWord(w); cNode == nil {
			break
		}
	}
	if cNode != nil {
		cNode.Valid = false
	}
}

func (ngdb *NgramDB) FindNgrams(partialDoc string, globalStartIdx int) []NgramResult {
	sz := len(partialDoc)

	/*if strings.HasPrefix(partialDoc, "s s") {
		fmt.Fprintln(os.Stderr, "find ngrams", partialDoc, ngdb.Trie
		os.Exit(1)
	}*/

	// skip leading spaces
	start := 0
	for ; start < sz && partialDoc[start] == ' '; start += 1 {
	}

	end := start
	cNode := &ngdb.Trie.Root
	results := make([]NgramResult, 0, 4)
	for start < sz {
		// find the end of the word
		for end = start; end < sz && partialDoc[end] != ' '; end += 1 {
		}

		if start == end { // start == end == sz
			break
		}

		//fmt.Fprintln(os.Stderr, "find ngrams", partialDoc[:end], partialDoc[start:end], len(partialDoc[start:end]))

		cNode = cNode.FindWord(partialDoc[start:end])
		if cNode == nil {
			break
		}

		if cNode.Valid {
			//fmt.Fprintln(os.Stderr, "find ngrams valid", partialDoc[:end])

			results = append(results, NgramResult{
				Term: partialDoc[:end],
				Pos:  globalStartIdx + start,
			})
		}

		for start = end; start < sz && partialDoc[start] == ' '; start += 1 {
		}
	}

	return results
}
