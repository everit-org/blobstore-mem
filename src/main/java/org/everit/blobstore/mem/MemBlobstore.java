package org.everit.blobstore.mem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.everit.blobstore.api.BlobAccessor;
import org.everit.blobstore.api.BlobReader;
import org.everit.blobstore.api.Blobstore;
import org.everit.blobstore.api.BlobstoreException;

/**
 * Memory based transactional implementation of {@link Blobstore} that should be used only for
 * testing purposes.
 */
public class MemBlobstore implements Blobstore {

  /**
   * Metadata and content of blobs.
   */
  static class BlobData {
    public List<Byte> content;

    public final ReentrantLock lock = new ReentrantLock();

    public long version;

    public BlobData(final long version, final List<Byte> content) {
      this.version = version;
      this.content = content;
    }

  }

  /**
   * An {@link XAResource} implementation that updates the blob data in case of a successful commit
   * and releases the lock of that belongs to the blob on the end of the transaction.
   */
  private final class BlobManipulationXAResource implements XAResource {
    private final long blobId;

    private final ReentrantLock lock;

    private final Consumer<T>

    private final BlobData newData;

    private BlobManipulationXAResource(final long blobId, final ReentrantLock lock,
        final BlobData newData) {
      this.blobId = blobId;
      this.lock = lock;
      this.newData = newData;
    }

    @Override
    public void commit(final Xid xid, final boolean onePhase) throws XAException {
      blobs.put(blobId, newData);
      if (lock != null) {
        lock.unlock();
      }
    }

    @Override
    public void end(final Xid xid, final int flags) throws XAException {
    }

    @Override
    public void forget(final Xid xid) throws XAException {
    }

    @Override
    public int getTransactionTimeout() throws XAException {
      return 0;
    }

    @Override
    public boolean isSameRM(final XAResource xares) throws XAException {
      return false;
    }

    @Override
    public int prepare(final Xid xid) throws XAException {
      return 0;
    }

    @Override
    public Xid[] recover(final int flag) throws XAException {
      return null;
    }

    @Override
    public void rollback(final Xid xid) throws XAException {
      if (lock != null) {
        lock.unlock();
      }
    }

    @Override
    public boolean setTransactionTimeout(final int seconds) throws XAException {
      return false;
    }

    @Override
    public void start(final Xid xid, final int flags) throws XAException {
    }
  }

  private final Map<Long, BlobData> blobs = new ConcurrentHashMap<>();

  private final AtomicLong nextBlobId = new AtomicLong();

  private final TransactionManager transactionManager;

  public MemBlobstore(final TransactionManager transactionManager) {
    this.transactionManager = transactionManager;
  }

  @Override
  public long createBlob(final Consumer<BlobAccessor> createAction) {
    BlobData data = new BlobData(0, new ArrayList<>());
    MemBlobAccessorImpl blobAccessor = new MemBlobAccessorImpl(data, 0, false);
    long blobId = nextBlobId.getAndIncrement();
    if (createAction != null) {
      createAction.accept(blobAccessor);
    }
    blobs.put(blobId, data);
    return blobId;
  }

  @Override
  public void deleteBlob(final long blobId) {
    blobs.remove(blobId);
  }

  private BlobData getBlobDataForUpdateAndLock(final long blobId) {
    BlobData blobData = blobs.get(blobId);
    if (blobData == null) {
      throw new BlobstoreException("No blob available with id " + blobId);
    }
    BlobData blobDataInLock = null;
    while (!blobData.equals(blobDataInLock)) {
      blobData.lock.lock();
      blobDataInLock = blobs.get(blobId);
      if (blobDataInLock == null) {
        blobData.lock.unlock();
        throw new BlobstoreException("No blob available with id " + blobId);
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
    int transactionStatus;
    try {
      transactionStatus = transactionManager.getStatus();
    } catch (SystemException e) {
      throw new BlobstoreException("Could not update blob due to transactional system exception",
          e);
    }

    BlobData data = getBlobDataForUpdateAndLock(blobId);

    BlobData newData = new BlobData(data.version + 1, new ArrayList<Byte>(data.content));

    if (transactionStatus == Status.STATUS_NO_TRANSACTION) {
      try {
        updatingAction.accept(new MemBlobAccessorImpl(newData, data.version, false));
        blobs.put(blobId, newData);
      } finally {
        data.lock.unlock();
      }
    } else {
      try {
        Transaction transaction = transactionManager.getTransaction();
        transaction.enlistResource(new BlobManipulationXAResource(blobId, data.lock, newData));
      } catch (SystemException | IllegalStateException | RollbackException e) {
        if (data.lock.isLocked()) {
          data.lock.unlock();
        }
        throw new BlobstoreException("Error during updating blob", e);
      }
      updatingAction.accept(new MemBlobAccessorImpl(newData, data.version, false));
    }
  }
}
