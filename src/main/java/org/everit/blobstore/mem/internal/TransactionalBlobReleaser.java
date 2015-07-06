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

import java.util.concurrent.locks.Lock;

/**
 * Helper interface to be able to lock a blob during update and delete until the end of the
 * transaction.
 */
public interface TransactionalBlobReleaser {

  /**
   * Unlocks the provided lock in the end of the transaction, after the blobs map is updated and
   * checks if the blob accessor is closed.
   *
   * @param lock
   *          Optional lock that locks the manipulation of a specific blob.
   * @throws IllegalStateException
   *           if the blob accessor is not closed.
   */
  void releaseBlobAfterCommitOrRollback(MemBlobAccessorImpl blobAccessor, Lock lock);
}
