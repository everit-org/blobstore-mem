/*
 * Copyright (C) 2011 Everit Kft. (http://www.everit.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.everit.blobstore.mem;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.everit.blobstore.api.BlobAccessor;
import org.everit.blobstore.api.BlobReader;
import org.everit.blobstore.api.Blobstore;
import org.everit.blobstore.api.NoSuchBlobException;
import org.everit.blobstore.mem.internal.MemBlobAccessorImpl;
import org.everit.blobstore.mem.internal.MemBlobData;
import org.everit.blobstore.mem.internal.MemBlobsMap;
import org.everit.blobstore.mem.internal.TransactionalBlobReleaser;
import org.everit.transaction.map.managed.ManagedMap;
import org.everit.transaction.unchecked.UncheckedSystemException;

/**
 * Memory based transactional implementation of {@link Blobstore} that should be used only for
 * testing purposes.
 */
public class MemBlobstore implements Blobstore {

  private final ManagedMap<Long, MemBlobData> blobs;

  private final AtomicLong nextBlobId = new AtomicLong();

  private final TransactionalBlobReleaser transactionalLockHolder;

  private final TransactionManager transactionManager;

  /**
   * Constructor.
   *
   * @param transactionManager
   *          The transaction manager that handles the transactions related to the blob.
   */
  public MemBlobstore(final TransactionManager transactionManager) {
    MemBlobsMap blobsMap = new MemBlobsMap();
    blobs = new ManagedMap<>(blobsMap, transactionManager);
    this.transactionalLockHolder = blobsMap;
    this.transactionManager = transactionManager;
  }

  private void checkActiveTransaction() {
    try {
      int status = transactionManager.getStatus();
      if (status != Status.STATUS_ACTIVE) {
        throw new IllegalStateException("Blobs can be manipulated only within active transactions");
      }
    } catch (SystemException e) {
      throw new UncheckedSystemException(e);
    }

  }

  @Override
  public BlobAccessor createBlob() {
    checkActiveTransaction();
    MemBlobData data = new MemBlobData(0, new ArrayList<>());
    long blobId = nextBlobId.getAndIncrement();
    MemBlobAccessorImpl blobAccessor = new MemBlobAccessorImpl(blobId, data, 0, false);
    blobs.put(blobId, data);
    transactionalLockHolder.releaseBlobAfterCommitOrRollback(blobAccessor, null);
    return blobAccessor;
  }

  @Override
  public void deleteBlob(final long blobId) {
    checkActiveTransaction();
    MemBlobData blobData = getBlobDataForUpdateAndLock(blobId);
    if (blobData == null) {
      throw new NoSuchBlobException(blobId);
    }
    try {
      blobs.remove(blobId);
    } finally {
      transactionalLockHolder.releaseBlobAfterCommitOrRollback(null, blobData.lock);
    }
  }

  private MemBlobData getBlobDataForUpdateAndLock(final long blobId) {
    MemBlobData blobData = blobs.get(blobId);
    if (blobData == null) {
      throw new NoSuchBlobException(blobId);
    }
    MemBlobData blobDataInLock = null;
    while (!blobData.equals(blobDataInLock)) {
      blobData.lock.lock();
      blobDataInLock = blobs.get(blobId);
      if (blobDataInLock == null) {
        blobData.lock.unlock();
        throw new NoSuchBlobException(blobId);
      }
      if (!blobData.equals(blobDataInLock)) {
        blobData.lock.unlock();
        blobData = blobDataInLock;
        blobDataInLock = null;
      }
    }
    return blobData;
  }

  @Override
  public BlobReader readBlob(final long blobId) {
    checkActiveTransaction();
    MemBlobData data = blobs.get(blobId);
    if (data == null) {
      throw new NoSuchBlobException(blobId);
    }
    MemBlobAccessorImpl blobReader = new MemBlobAccessorImpl(blobId, data, data.version, true);
    transactionalLockHolder.releaseBlobAfterCommitOrRollback(blobReader, null);
    return blobReader;
  }

  @Override
  public BlobReader readBlobForUpdate(final long blobId) {
    checkActiveTransaction();

    MemBlobData blobData = getBlobDataForUpdateAndLock(blobId);

    if (blobData == null) {
      throw new NoSuchBlobException(blobId);
    }

    MemBlobAccessorImpl blobAccessorImpl =
        new MemBlobAccessorImpl(blobId, blobData, blobData.version, true);

    transactionalLockHolder.releaseBlobAfterCommitOrRollback(blobAccessorImpl, blobData.lock);

    return blobAccessorImpl;
  }

  @Override
  public BlobAccessor updateBlob(final long blobId) {
    checkActiveTransaction();
    MemBlobData blobData = getBlobDataForUpdateAndLock(blobId);

    if (blobData == null) {
      throw new NoSuchBlobException(blobId);
    }

    MemBlobData newBlobData = new MemBlobData(blobData.version + 1,
        new ArrayList<Byte>(blobData.content), blobData.lock);

    blobs.put(blobId, newBlobData);

    MemBlobAccessorImpl blobAccessor =
        new MemBlobAccessorImpl(blobId, newBlobData, blobData.version, false);

    transactionalLockHolder.releaseBlobAfterCommitOrRollback(blobAccessor, blobData.lock);

    return blobAccessor;

  }
}
