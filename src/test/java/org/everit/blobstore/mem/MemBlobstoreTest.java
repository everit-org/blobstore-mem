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
import org.everit.blobstore.Blobstore;
import org.everit.blobstore.testbase.AbstractBlobstoreTest;
import org.everit.osgi.transaction.helper.internal.JTATransactionPropagator;
import org.everit.transaction.propagator.TransactionPropagator;
import org.junit.Before;

public class MemBlobstoreTest extends AbstractBlobstoreTest {

  private MemBlobstore memBlobstore;

  private JTATransactionPropagator transactionPropagator;

  private TransactionManager transactionManager;

  @Before
  public void before() {
    try {
      transactionManager = new GeronimoTransactionManager();
    } catch (XAException e) {
      throw new RuntimeException(e);
    }
    transactionPropagator = new JTATransactionPropagator(transactionManager);

    memBlobstore = new MemBlobstore(transactionManager);
  }

  @Override
  protected Blobstore getBlobStore() {
    return memBlobstore;
  }

  @Override
  protected TransactionPropagator getTransactionPropagator() {
    return transactionPropagator;
  }

}
