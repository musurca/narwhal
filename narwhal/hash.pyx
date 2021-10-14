"""	Calculates an order-independent hash.

	Any collection of identical characters will return 
	the same hash, regardless of the order in which 
	they're arranged.
"""

def order_ind_hash(cmd_str:str):
	cdef unsigned long hash, k
	cdef int i
	cdef int str_len = len(cmd_str)

	hash = 0
	for i in range( str_len ):
		k = ord(cmd_str[i])
		k = k ^ (k >> 17)
		k *= 830770091
		k = k ^ (k >> 11)
		k *= -1404298415
		k = k ^ (k >> 15)
		k *= 830770091
		k = k ^ (k >> 14)
		hash += k
	return hash