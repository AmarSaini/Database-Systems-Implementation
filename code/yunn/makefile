CC = g++ -g -O0 -Wno-deprecated

tag = -i

ifdef linux
	tag = -n
endif


test.out:	BPlusTree.o BPlusNode.o test.o
	$(CC) -o test.out test.o BPlusTree.o BPlusNode.o
	
test.o:	test.cpp
	$(CC) -c test.cpp

BPlusTree.o: BPlusTree.h Utility.h BPlusTree.cc
	$(CC) -c BPlusTree.cc

BPlusNode.o: BPlusNode.h Utility.h BPlusNode.cc
	$(CC) -c BPlusNode.cc

clean: 
	rm -f *.o
	rm -f *.out