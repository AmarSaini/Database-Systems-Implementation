#ifndef _BPLUSNODE_H
#define _BPLUSNODE_H
/*	B+ Tree Data Structure
*	Handles Basic B+ Tree functionality
*  Only supports Key of type int and float as defined by c++
*/
#include <cstddef>
#include <iostream>
#include "Utility.h"

enum nodeType { INTERNAL = 0, LEAF };

struct BNode {

	int keyCount;
	BNode* parent;
	nodeType type;

	BNode();
	virtual void print();
};

struct leafNodeData {
	int key;
	int pageNum;
	int recordNum;

	leafNodeData* next, *parent;

	leafNodeData();
	leafNodeData(int key = -1, int pageNum = -1, int recordNum = -1);
	void update(int key, int pageNum, int recordNum);
	void insert(leafNodeData* toInsert);
	void Swap(leafNodeData* toSwap);
	void print();
	leafNodeData* steal(int keyCount, int stealCount);
};

// B+ Tree Representation of Leaf Node
struct leafNode : public BNode {
	leafNodeData* data;
	leafNode* next;

	leafNode();
	~leafNode();
	virtual void print();
	void insert(int key, int pageNum, int recordNum);
	leafNode* split();
};
struct internNodeData {
	int key;
	BNode* child;
	int pageNum;
	internNodeData* next, *parent;

	internNodeData(int key = -1, int pageNum = -1, BNode* child = NULL);
	~internNodeData();
	void update(int key, BNode* child, int pageNum);
	void insert(internNodeData* toInsert);
	void Swap(internNodeData* toSwap);
	void print();
};

// B+ Tree Representation of Internal Node
struct internalNode : public BNode {
	internNodeData* data;
	internNodeData* first;

	internalNode(int size);
	~internalNode();
	virtual void print();
	void insert(int key, int pageNum, leafNode* child);
	internalNode* split();
};
#endif