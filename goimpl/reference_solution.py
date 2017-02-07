#!/usr/bin/env python
import sys

ngrams = []
initial_input = True
while 1:
    line = sys.stdin.readline()
    if not line:
        break
    elif initial_input:
        if line[0] == "S":
            initial_input = False
            print 'R'
            sys.stdout.flush()
        else:
            ngrams.append(line[0:-1])
    else:
        if line[0] == "D":
            ngrams.remove(line[2:-1])
        elif line[0] == "A":    
            ngrams.append(line[2:-1])
        elif line[0] == "Q":
            text = " " + line[2:-1] + " "
            results = []
            for ngram in ngrams:
                att = text.find(" " + ngram + " ", 0)
                if att != -1:
                    results.append((ngram, att))

            if len(results) == 0:
                print -1
            else:
                s = sorted(results, key=lambda x: (x[1],len(x[0])))
                for cnt, term in enumerate(s):
                    if cnt == 0:
                        sys.stdout.write(term[0])
                    else:
                        sys.stdout.write('|' + term[0])
                sys.stdout.write('\n')
                sys.stdout.flush()
        elif line[0] == "F":
            continue

