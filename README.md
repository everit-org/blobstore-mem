# blobstore-mem

Transactional in-memory based implementation of [Everit Blobstore API][1].

## Usage

    // Get a JTA transactionManager from somewhere
    TransactionManager transactionManager = getTransactionManager();
    
    // Instantiate the memory-based blobstore
    Blobstore blobstore = new MemBlobstore(transactionManager);

After that, you can use the _cachedBlobstore_ instance as it is described
in the documentation of [blobstore-api][1].

[1]: https://github.com/everit-org/blobstore-api

