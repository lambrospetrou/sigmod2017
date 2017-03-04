#Profiling CFLAGS
# march=core2 
# march=corei7-avx
MATH_FLAGS=-ffast-math -funsafe-math-optimizations -fassociative-math -ffinite-math-only -fno-signed-zeros -funsafe-loop-optimizations -ftree-loop-if-convert-stores
RELEASE_CFLAGS=-march=native -std=c++11 -Ofast -O3 -W -Wall -Wextra -Wunused $(MATH_FLAGS) -fno-builtin -ftree-vectorize -funroll-all-loops -fvariable-expansion-in-unroller -fomit-frame-pointer -freorder-blocks-and-partition -Iinclude 

COMPILE_CMD=g++ -g -L./include/asm  $(RELEASE_CFLAGS) -o$@ main.cpp -lpthread -fopt-info-vec #-fopt-info-vec-missed -fabi-version=0 

all: main

# just compile always anyway ;)
main: main.cpp; 
	${COMPILE_CMD}

clean:
	rm main
