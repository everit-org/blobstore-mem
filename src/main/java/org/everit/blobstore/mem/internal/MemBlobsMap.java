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
package org.everit.blobstore.mem.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import org.everit.transaction.map.TransactionalMap;
import org.everit.transaction.map.readcommited.ReadCommitedTransactionalMap;

/**
 * Transactional Map that helps managing locks during blob update.
 */
public class MemBlobsMap implements TransactionalMap<Long, MemBlobData>, TransactionalBlobReleaser {

  private final Map<Object, List<BlobAccessorAndLock>> blobAccessorAndLockByTransactionMap =
      new ConcurrentHashMap<>();

  private final TransactionalMap<Long, MemBlobData> wrapped =
      new ReadCommitedTransactionalMap<>(null);

  @Override
  public void clear() {
    wrapped.clear();
  }

  @Override
  public void commitTransaction() {
    Object transaction = wrapped.getAssociatedTransaction();
    try {
      wrapped.commitTransaction();
    } finally {
      releaseAllBlobsForTransaction(transaction);
    }
  }

  @Override
  public boolean containsKey(final Object key) {
    return wrapped.containsKey(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    return wrapped.containsValue(value);
  }

  @Override
  public Set<Entry<Long, MemBlobData>> entrySet() {
    return wrapped.entrySet();
  }

  @Override
  public boolean equals(final Object o) {
    return wrapped.equals(o);
  }

  @Override
  public MemBlobData get(final Object key) {
    return wrapped.get(key);
  }

  @Override
  public Object getAssociatedTransaction() {
    return wrapped.getAssociatedTransaction();
  }

  @Override
  public int hashCode() {
    return wrapped.hashCode();
  }

  @Override
  public boolean isEmpty() {
    return wrapped.isEmpty();
  }

  @Override
  public Set<Long> keySet() {
    return wrapped.keySet();
  }

  @Override
  public MemBlobData put(final Long key, final MemBlobData value) {
    return wrapped.put(key, value);
  }

  @Override
  public void putAll(final Map<? extends Long, ? extends MemBlobData> m) {
    wrapped.putAll(m);
  }

  private void releaseAllBlobsForTransaction(final Object transaction) {
    List<BlobAccessorAndLock> blobAcessorAndLockList =
        blobAccessorAndLockByTransactionMap.remove(transaction);

    if (blobAcessorAndLockList != null) {
      for (BlobAccessorAndLock blobAccessorAndLock : blobAcessorAndLockList) {
        if (blobAccessorAndLock.lock != null) {
          blobAccessorAndLock.lock.unlock();
        }
        if (blobAccessorAndLock.blobAccessor != null
            && !blobAccessorAndLock.blobAccessor.isClosed()) {
          throw new IllegalStateException(
              "Blob accessor should have been closed before ending the transaction");
        }
      }
    }
  }

  @Override
  public void releaseBlobAfterCommitOrRollback(final MemBlobAccessorImpl blobAccessor,
      final Lock lock) {
    Object transaction = wrapped.getAssociatedTransaction();
    List<BlobAccessorAndLock> list = blobAccessorAndLockByTransactionMap.get(transaction);
    if (list == null) {
      list = new ArrayList<>();
      blobAccessorAndLockByTransactionMap.put(transaction, list);
    }
    list.add(new BlobAccessorAndLock(blobAccessor, lock));
  }

  @Override
  public MemBlobData remove(final Object key) {
    return wrapped.remove(key);
  }

  @Override
  public void resumeTransaction(final Object transaction) {
    wrapped.resumeTransaction(transaction);
  }

  @Override
  public void rollbackTransaction() {
    Object transaction = wrapped.getAssociatedTransaction();
    releaseAllBlobsForTransaction(transaction);
    wrapped.rollbackTransaction();
  }

  @Override
  public int size() {
    return wrapped.size();
  }

  @Override
  public void startTransaction(final Object transaction) {
    wrapped.startTransaction(transaction);
  }

  @Override
  public void suspendTransaction() {
    wrapped.suspendTransaction();
  }

  @Override
  public Collection<MemBlobData> values() {
    return wrapped.values();
  }

}
