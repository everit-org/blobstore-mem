package org.everit.blobstore.mem;

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;

import org.apache.geronimo.transaction.manager.GeronimoTransactionManager;
import org.everit.blobstore.api.Blobstore;
import org.everit.blobstore.testbase.AbstractBlobstoreTest;
import org.everit.osgi.transaction.helper.api.TransactionHelper;
import org.everit.osgi.transaction.helper.internal.TransactionHelperImpl;
import org.junit.Before;

public class BlobstoreTest extends AbstractBlobstoreTest {

  private MemBlobstore memBlobstore;

  private TransactionHelperImpl transactionHelper;

  private TransactionManager transactionManager;

  @Before
  public void before() {
    try {
      transactionManager = new GeronimoTransactionManager();
    } catch (XAException e) {
      throw new RuntimeException(e);
    }
    transactionHelper = new TransactionHelperImpl();
    transactionHelper.setTransactionManager(transactionManager);

    memBlobstore = new MemBlobstore(transactionManager);
  }

  @Override
  protected Blobstore getBlobStore() {
    return memBlobstore;
  }

  @Override
  protected TransactionHelper getTransactionHelper() {
    return transactionHelper;
  }

}
