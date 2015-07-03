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
public class BlobsMap implements TransactionalMap<Long, BlobData>, TransactionalLockHolder {

  private final Map<Object, List<Lock>> lockByTransactionMap = new ConcurrentHashMap<>();

  private final TransactionalMap<Long, BlobData> wrapped = new ReadCommitedTransactionalMap<>(null);

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
      releaseAllLocksForTransaction(transaction);
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
  public Set<Entry<Long, BlobData>> entrySet() {
    return wrapped.entrySet();
  }

  @Override
  public boolean equals(final Object o) {
    return wrapped.equals(o);
  }

  @Override
  public BlobData get(final Object key) {
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
  public BlobData put(final Long key, final BlobData value) {
    return wrapped.put(key, value);
  }

  @Override
  public void putAll(final Map<? extends Long, ? extends BlobData> m) {
    wrapped.putAll(m);
  }

  private void releaseAllLocksForTransaction(final Object transaction) {
    List<Lock> locks = lockByTransactionMap.remove(transaction);

    if (locks != null) {
      for (Lock lock : locks) {
        lock.unlock();
      }
    }
  }

  @Override
  public void releaseLockAfterCommit(final Lock lock) {
    Object transaction = wrapped.getAssociatedTransaction();
    List<Lock> locks = lockByTransactionMap.get(transaction);
    if (locks == null) {
      locks = new ArrayList<>();
      lockByTransactionMap.put(transaction, locks);
    }
    locks.add(lock);
  }

  @Override
  public BlobData remove(final Object key) {
    return wrapped.remove(key);
  }

  @Override
  public void resumeTransaction(final Object transaction) {
    wrapped.resumeTransaction(transaction);
  }

  @Override
  public void rollbackTransaction() {
    Object transaction = wrapped.getAssociatedTransaction();
    releaseAllLocksForTransaction(transaction);
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
  public Collection<BlobData> values() {
    return wrapped.values();
  }

}
