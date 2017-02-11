marios-reference: marios-reference.cpp threadpool.o
	g++ -O3 -std=c++11 -o marios-reference threadpool.o marios-reference.cpp -lpthread

threadpool.o: threadpool.h threadpool.cpp 
	gcc -c threadpool.cpp
