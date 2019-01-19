#ifndef _RecordMinHeap_H
#define _RecordMinHeap_H

#include "Record.h"
#include "Comparison.h"
#include <vector>

using namespace std;

struct HeapNode {
	Record data;
	int index;
};
class MinHeap
{
private:
	OrderMaker x;
	vector<HeapNode*> y;

private:
	void minHeapify(int i);
	void buildHeap();

public:
	MinHeap(OrderMaker order);
	// Delete everything inside heap y if its not empty
	~MinHeap();
	int getHeapSize();
	/* Creates a new HeapNode with data = rec and index = ind
	 * Pushes this new HeapNode into Heap 
	*/
	void insert(Record& rec, int ind);
	/* Builds the heap and extract (remove) the minimum heapNode from the heap
	 * Returns the extracted heapNode if successful or NULL if fail
	 * success is overwritten to 
	 * 1 if Heap Extraction was successful
	 * 0 if Heap Extraction failed (Heap is empty)
	*/

	HeapNode* extractMin(int& success);
};
#endif