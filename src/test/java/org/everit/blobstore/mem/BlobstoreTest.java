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
