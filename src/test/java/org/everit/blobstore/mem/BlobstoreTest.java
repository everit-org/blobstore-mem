package org.everit.blobstore.mem;

import javax.transaction.SystemException;

import org.everit.blobstore.api.Blobstore;
import org.everit.osgi.transaction.helper.api.TransactionHelper;
import org.everit.osgi.transaction.helper.internal.TransactionHelperImpl;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.atomikos.icatch.jta.UserTransactionManager;

public class BlobstoreTest {

  private static final class BooleanHolder {
    public boolean value = false;

    public BooleanHolder(final boolean value) {
      this.value = value;
    }
  }

  private MemBlobstore memBlobstore;

  private TransactionHelperImpl transactionHelper;

  private UserTransactionManager transactionManager;

  @After
  public void after() {
    transactionManager.close();
  }

  @Before
  public void before() {
    transactionManager = new UserTransactionManager();
    try {
      transactionManager.init();
    } catch (SystemException e) {
      throw new RuntimeException(e);
    }
    transactionHelper = new TransactionHelperImpl();
    transactionHelper.setTransactionManager(transactionManager);

    memBlobstore = new MemBlobstore(transactionManager);
  }

  protected Blobstore getBlobStore() {
    return memBlobstore;
  }

  protected TransactionHelper getTransactionHelper() {
    return transactionHelper;
  }

  private void notifyWaiter(final BooleanHolder shouldWait) {
    synchronized (shouldWait) {
      shouldWait.value = false;
      shouldWait.notifyAll();
    }
  }

  @Test
  public void test01ZeroLengthBlob() {
    Blobstore blobStore = getBlobStore();
    long blobId = blobStore.createBlob((blobAccessor) -> {
    });

    blobStore.readBlob(blobId, (blobReader) -> {
      Assert.assertEquals(0, blobReader.size());
    });
  }

  @Test
  public void test02BlobCreationWithContent() {
    Blobstore blobStore = getBlobStore();
    long blobId = blobStore.createBlob((blobAccessor) -> {
      blobAccessor.write(new byte[] { 2, 1, 2, 3 }, 1, 2);
    });

    blobStore.readBlob(blobId, (blobReader) -> {
      Assert.assertEquals(2, blobReader.size());
      byte[] buffer = new byte[5];
      int read = blobReader.read(buffer, 1, 3);
      Assert.assertEquals(2, read);
      Assert.assertArrayEquals(new byte[] { 0, 1, 2, 0, 0 }, buffer);
    });
  }

  @Test
  public void test03UpdateParallelBlobWithoutTransaction() {
    Blobstore blobStore = getBlobStore();
    long blobId = blobStore.createBlob((blobAccessor) -> {
      blobAccessor.write(new byte[] { 0 }, 0, 1);
    });

    BooleanHolder waitForReadCheck = new BooleanHolder(true);
    BooleanHolder waitForUpdate = new BooleanHolder(true);

    new Thread(() -> {
      blobStore.updateBlob(blobId, (blobAccessor) -> {
        blobAccessor.seek(blobAccessor.size());
        blobAccessor.write(new byte[] { 1 }, 0, 1);
        waitIfTrue(waitForReadCheck);
      });
      notifyWaiter(waitForUpdate);
    }).start();

    // Do some test read before update finishes
    blobStore.readBlob(blobId, (blobReader) -> {
      Assert.assertEquals(1, blobReader.size());
    });

    // Create another blob until lock of first blob holds
    long blobIdOfSecondBlob = blobStore.createBlob(null);
    blobStore.readBlob(blobIdOfSecondBlob, (blobReader) -> {
      Assert.assertEquals(0, blobReader.size());
    });
    blobStore.deleteBlob(blobIdOfSecondBlob);

    notifyWaiter(waitForReadCheck);
    waitIfTrue(waitForUpdate);

    blobStore.readBlob(blobId, (blobReader) -> Assert.assertEquals(2, blobReader.size()));

    blobStore.deleteBlob(blobId);
  }

  @Test
  public void test04UpdateParallelBlobWithTransaction() {
    Blobstore blobStore = getBlobStore();
    long blobId = blobStore.createBlob((blobAccessor) -> {
      blobAccessor.write(new byte[] { 0 }, 0, 1);
    });

    transactionHelper.required(() -> {
      blobStore.updateBlob(blobId, (blobAccessor) -> {
        blobAccessor.seek(blobAccessor.size());
        blobAccessor.write(new byte[] { 1 }, 0, 1);
      });
      blobStore.readBlob(blobId, (blobReader) -> Assert.assertEquals(1, blobReader.size()));
      long blobId2 = blobStore.createBlob(null);
      transactionHelper.requiresNew(() -> {
        blobStore.updateBlob(blobId2, (blobAccessor) -> {

        });
        return null;
      });

      blobStore.deleteBlob(blobId2);
      return null;
    });
    blobStore.readBlob(blobId, (blobReader) -> Assert.assertEquals(2, blobReader.size()));

    blobStore.deleteBlob(blobId);
  }

  private void waitIfTrue(final BooleanHolder shouldWait) {
    while (shouldWait.value) {
      synchronized (shouldWait) {
        if (shouldWait.value) {
          try {
            shouldWait.wait();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }
  }
}
