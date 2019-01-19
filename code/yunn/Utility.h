#ifndef PRIMITIVESWAP
#define PRIMITIVESWAP
// Swap Template Function 
template <typename T>
void SWAP(T& x, T& y)
{
	T temp = x;
	x = y;
	y = temp;
}
#endif // !PRIMITIVESWAP

