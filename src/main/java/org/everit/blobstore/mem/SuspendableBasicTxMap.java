package org.everit.blobstore.mem;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.transaction.memory.BasicTxMap;

/**
 * Transactional map for storing blobs.
 */
class SuspendableBasicTxMap<K, V> extends BasicTxMap<K, V> {

  private final Map<Object, MapTxContext> suspendedTXContexts = new ConcurrentHashMap<>();

  public SuspendableBasicTxMap(final String name) {
    super(name);
  }

  /**
   * Resumes the transaction with the specified id.
   *
   * @param transaction
   *          The transaction to resume.
   */
  public void resumeTransaction(final Object transaction) {
    MapTxContext txContext = suspendedTXContexts.remove(transaction);
    Objects.requireNonNull(txContext);
    setActiveTx(txContext);
  }

  /**
   * Suspends the current transaction.
   *
   * @param transaction
   *          The transaction that will be used to resume.
   */
  public void suspendTransaction(final Object transaction) {
    @SuppressWarnings("unchecked")
    MapTxContext activeTx = getCheckedActiveTx();
    suspendedTXContexts.put(transaction, activeTx);
    setActiveTx(null);
  }

}
