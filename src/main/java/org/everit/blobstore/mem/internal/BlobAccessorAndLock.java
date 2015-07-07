package org.everit.blobstore.mem.internal;

import java.util.concurrent.locks.Lock;

/**
 * Holder class for blob accessor and a lock that is held for the blob. Both can be
 * <code>null</code> depending on the use-case.
 *
 */
public class BlobAccessorAndLock {

  public final MemBlobAccessorImpl blobAccessor;

  public final Lock lock;

  public BlobAccessorAndLock(final MemBlobAccessorImpl blobAccessor, final Lock lock) {
    this.blobAccessor = blobAccessor;
    this.lock = lock;
  }

}
