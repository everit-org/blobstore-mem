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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.transaction.TransactionManager;

import org.everit.blobstore.api.BlobAccessor;
import org.everit.blobstore.api.BlobReader;
import org.everit.blobstore.api.Blobstore;
import org.everit.blobstore.api.BlobstoreException;
import org.everit.blobstore.api.NoSuchBlobException;
import org.everit.blobstore.mem.internal.BlobData;
import org.everit.blobstore.mem.internal.BlobsMap;
import org.everit.blobstore.mem.internal.MemBlobAccessorImpl;
import org.everit.blobstore.mem.internal.TransactionalLockHolder;
import org.everit.osgi.transaction.helper.api.TransactionHelper;
import org.everit.osgi.transaction.helper.internal.TransactionHelperImpl;
import org.everit.transaction.map.managed.ManagedMap;

/**
 * Memory based transactional implementation of {@link Blobstore} that should be used only for
 * testing purposes.
 */
public class MemBlobstore implements Blobstore {

  private final ManagedMap<Long, BlobData> blobs;

  private final AtomicLong nextBlobId = new AtomicLong();

  private final TransactionalLockHolder transactionalLockHolder;

  private final TransactionHelper transactionHelper;

  /**
   * Constructor.
   *
   * @param transactionManager
   *          The transaction manager that handles the transactions related to the blob.
   */
  public MemBlobstore(final TransactionManager transactionManager) {
    BlobsMap blobsMap = new BlobsMap();
    blobs = new ManagedMap<>(blobsMap, transactionManager);
    this.transactionalLockHolder = blobsMap;
    TransactionHelperImpl transactionHelperImpl = new TransactionHelperImpl();
    transactionHelperImpl.setTransactionManager(transactionManager);
    this.transactionHelper = transactionHelperImpl;

  }

  @Override
  public long createBlob(final Consumer<BlobAccessor> createAction) {
    BlobData data = new BlobData(0, new ArrayList<>());
    MemBlobAccessorImpl blobAccessor = new MemBlobAccessorImpl(data, 0, false);
    long blobId = nextBlobId.getAndIncrement();
    transactionHelper.required(() -> {
      if (createAction != null) {
        createAction.accept(blobAccessor);
      }
      blobs.put(blobId, data);
      return null;
    });
    return blobId;
  }

  @Override
  public void deleteBlob(final long blobId) {
    BlobData blobData = getBlobDataForUpdateAndLock(blobId);
    if (blobData == null) {
      throw new NoSuchBlobException(blobId);
    }
    transactionHelper.required(() -> {
      try {
        blobs.remove(blobId);
      } finally {
        transactionalLockHolder.releaseLockAfterCommit(blobData.lock);
      }
      return null;
    });
  }

  private BlobData getBlobDataForUpdateAndLock(final long blobId) {
    BlobData blobData = blobs.get(blobId);
    if (blobData == null) {
      throw new NoSuchBlobException(blobId);
    }
    BlobData blobDataInLock = null;
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
  public void readBlob(final long blobId, final Consumer<BlobReader> readingAction) {
    BlobData data = blobs.get(blobId);
    if (data == null) {
      throw new BlobstoreException("Blob not available");
    }
    readingAction.accept(new MemBlobAccessorImpl(data, data.version, true));
  }

  @Override
  public void updateBlob(final long blobId, final Consumer<BlobAccessor> updatingAction) {
    Objects.requireNonNull(updatingAction);

    BlobData blobData = getBlobDataForUpdateAndLock(blobId);

    transactionHelper.required(() -> {
      try {
        BlobData newBlobData = new BlobData(blobData.version + 1,
            new ArrayList<Byte>(blobData.content), blobData.lock);

        updatingAction.accept(new MemBlobAccessorImpl(newBlobData, blobData.version, false));
        blobs.put(blobId, newBlobData);
      } finally {
        transactionalLockHolder.releaseLockAfterCommit(blobData.lock);
      }
      return null;
    });
  }
}
