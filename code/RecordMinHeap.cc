#include "RecordMinHeap.h"
using namespace std;

void MinHeap::minHeapify(int i)
{
	int l = 2 * i;
	int r = 2 * i + 1;
	int minimum;
	if (l < y.size() && x.Run(y[l]->data, y[i]->data) == -1)
		minimum = l;
	else
		minimum = i;

	if (r < y.size() && x.Run(y[r]->data, y[minimum]->data) == -1)
		minimum = r;

	if (minimum != i)
	{
		std::swap(y[i], y[minimum]);
		minHeapify(minimum);
	}
}
int MinHeap::getHeapSize() {
	return y.size();
}

void MinHeap::buildHeap()
{
	for (int i = (y.size() - 1) / 2; i >= 0; i--)
		minHeapify(i);
}
MinHeap::MinHeap(OrderMaker order)
{
	x.Swap(order);
}
MinHeap::~MinHeap() {
	for (int i = 0; i < y.size(); ++i)
		delete y[i];
}
void MinHeap::insert(Record& rec, int ind)
{
	HeapNode* n = new HeapNode;
	n->data = rec;
	n->index = ind;
	y.push_back(n);
}
HeapNode* MinHeap::extractMin(int& success)
{
	if (y.size() == 0) {
		success = 0;
		return NULL;
	}
	buildHeap();
	HeapNode* temp = y[0];
	y.erase(y.begin());
	success = 1;
	return temp;
}
