package org.everit.blobstore.mem.internal;

import java.util.concurrent.locks.Lock;

public class BlobAccessorAndLock {

  public final MemBlobAccessorImpl blobAccessor;

  public final Lock lock;

  public BlobAccessorAndLock(final MemBlobAccessorImpl blobAccessor, final Lock lock) {
    this.blobAccessor = blobAccessor;
    this.lock = lock;
  }

}
