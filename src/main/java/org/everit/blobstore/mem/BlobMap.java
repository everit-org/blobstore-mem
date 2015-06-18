package org.everit.blobstore.mem;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.transaction.memory.BasicTxMap;
import org.everit.blobstore.mem.MemBlobstore.BlobData;

/**
 * Transactional map for storing blobs.
 */
class BlobMap extends BasicTxMap<Long, BlobData> {

  private final Map<Object, MapTxContext> suspendedTXContexts = new ConcurrentHashMap<>();

  public BlobMap(final String name) {
    super(name);
  }

  /**
   * Resumes the transaction with the specified id.
   * 
   * @param transactionId
   *          The id of the transaction to resume.
   */
  public void resumeTransaction(final Object transactionId) {
    MapTxContext txContext = suspendedTXContexts.remove(transactionId);
    Objects.requireNonNull(txContext);
    setActiveTx(txContext);
  }

  /**
   * Suspends the current transaction.
   *
   * @param idOfCurrentTransaction
   *          The id of the current transaction that will be used to resume.
   */
  public void suspendTransaction(final Object idOfCurrentTransaction) {
    @SuppressWarnings("unchecked")
    MapTxContext activeTx = getCheckedActiveTx();
    suspendedTXContexts.put(idOfCurrentTransaction, activeTx);
    setActiveTx(createContext());
  }

}
