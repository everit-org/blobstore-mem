package org.everit.blobstore.mem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.everit.blobstore.api.BlobAccessor;
import org.everit.blobstore.api.BlobReader;
import org.everit.blobstore.api.Blobstore;
import org.everit.blobstore.api.BlobstoreException;
import org.everit.blobstore.api.NoSuchBlobException;
import org.everit.osgi.transaction.helper.api.TransactionHelper;
import org.everit.osgi.transaction.helper.internal.TransactionHelperImpl;
import org.everit.transaction.managed.map.jta.ManagedMap;

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

  private final ManagedMap<Long, BlobData> blobs;

  private final AtomicLong nextBlobId = new AtomicLong();

  private final TransactionHelper transactionHelper;

  private final TransactionManager transactionManager;

  private final Map<Transaction, Boolean> transactionsEnlisted = new WeakHashMap<>();

  /**
   * Constructor.
   *
   * @param transactionManager
   *          The transaction manager that handles the transactions related to the blob.
   */
  public MemBlobstore(final TransactionManager transactionManager) {
    this.transactionManager = transactionManager;
    blobs = new ManagedMap<>(transactionManager);
    TransactionHelperImpl transactionHelperImpl = new TransactionHelperImpl();
    transactionHelperImpl.setTransactionManager(transactionManager);
    this.transactionHelper = transactionHelperImpl;
  }

  @Override
  public long createBlob(final Consumer<BlobAccessor> createAction) {
    BlobData data = new BlobData(0, new ArrayList<>());
    MemBlobAccessorImpl blobAccessor = new MemBlobAccessorImpl(data, 0, false);
    long blobId = nextBlobId.getAndIncrement();
    runManipulationAction(() -> {
      if (createAction != null) {
        createAction.accept(blobAccessor);
      }
      blobs.put(blobId, data);
    } , null);
    return blobId;
  }

  @Override
  public void deleteBlob(final long blobId) {
    BlobData blobData = getBlobDataForUpdateAndLock(blobId);
    if (blobData == null) {
      throw new NoSuchBlobException(blobId);
    }
    runManipulationAction(() -> blobs.remove(blobId), blobData.lock);

  }

  private boolean enlistBlobs(final Lock lock) {
    try {
      Transaction transaction = transactionManager.getTransaction();
      if (transactionsEnlisted.containsKey(transaction)) {
        return false;
      }
      transaction.enlistResource(new BlobManipulationXAResource(lock));
      transactionsEnlisted.put(transaction, Boolean.TRUE);
      return true;
    } catch (SystemException | IllegalStateException | RollbackException e) {
      throw new BlobstoreException(e);
    }

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

  private void runManipulationAction(final Runnable action, final ReentrantLock lock) {
    AtomicBoolean enlistedNow = new AtomicBoolean(false);
    try {
      transactionHelper.required(() -> {
        enlistedNow.set(enlistBlobs(lock));
        action.run();
        return null;
      });
    } catch (RuntimeException e) {
      if (enlistedNow.get() && lock != null && lock.isHeldByCurrentThread()) {
        lock.unlock();
      }
      throw e;
    }
  }

  @Override
  public void updateBlob(final long blobId, final Consumer<BlobAccessor> updatingAction) {
    Objects.requireNonNull(updatingAction);

    BlobData blobData = getBlobDataForUpdateAndLock(blobId);

    runManipulationAction(() -> {
      BlobData newBlobData = new BlobData(blobData.version + 1,
          new ArrayList<Byte>(blobData.content));

      updatingAction.accept(new MemBlobAccessorImpl(newBlobData, blobData.version, false));
      blobs.put(blobId, newBlobData);
    } , blobData.lock);
  }
}
