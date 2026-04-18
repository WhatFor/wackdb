# Buffer Pool Caching 

Reading and writing from disk is slow. Often, operations will need to be applied against memory and written to pages "later" for performance reasons.

This is where the Buffer Pool comes in. When a query asks for data, the engine doesn't read from the disk file but instead asks the Buffer Pool for the page. Ideally, the page is in memory. If not, the page is fetched. Fetching single pages across multiple I/O calls is slow, so batching the page reads using scatter/gather (Vectored) I/O is best; though not implemented.

The Buffer Pool uses an LRU cache.
