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

import org.everit.blobstore.BlobAccessor;

/**
 * Internal {@link BlobAccessor} implementation for {@link org.everit.blobstore.mem.MemBlobstore}.
 */
public class MemBlobAccessorImpl implements BlobAccessor {

  private final MemBlobData blobData;

  private final long blobId;

  private boolean closed = false;

  protected long position = 0;

  private final long previousVersion;

  private final boolean readOnly;

  /**
   * Constructor.
   *
   * @param data
   *          The metadata of the <code>BLOB</code>.
   * @param previousVersion
   *          The previous version of the <code>BLOB</code>. In case of readOnly mode or new
   *          <code>BLOBs</code> this number should be the same as {@link MemBlobData#version}.
   * @param readOnly
   *          Whether this <code>BLOB</code> manipulation is allowed via this accessor or not.
   */
  public MemBlobAccessorImpl(final long blobId, final MemBlobData data, final long previousVersion,
      final boolean readOnly) {

    this.blobId = blobId;
    this.blobData = data;
    this.previousVersion = previousVersion;
    this.readOnly = readOnly;
  }

  private void checkClosed() {
    if (closed) {
      throw new IllegalStateException("Blob is already closed");
    }
  }

  private void checkReadOnly() {
    if (readOnly) {
      throw new IllegalStateException("BLOB cannot be modified via reader.");
    }
  }

  @Override
  public void close() {
    this.closed = true;
  }

  @Override
  public long getBlobId() {
    return blobId;
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public long getNewVersion() {
    checkClosed();
    return blobData.version;
  }

  @Override
  public long getPosition() {
    checkClosed();
    return position;
  }

  @Override
  public int read(final byte[] b, final int off, final int len) {
    checkClosed();
    int n = (int) (getSize() - position);
    if (n == 0) {
      return -1;
    }
    if (len < n) {
      n = len;
    }

    int loff = off;
    for (int i = 0; i < n; i++) {
      b[loff] = blobData.content.get((int) position);
      loff++;
      position++;
    }
    return n;
  }

  @Override
  public void seek(final long pos) {
    checkClosed();
    if (pos > getSize() || pos < 0) {
      throw new IllegalArgumentException();
    }
    position = pos;
  }

  @Override
  public long getSize() {
    checkClosed();
    return blobData.content.size();
  }

  @Override
  public void truncate(final long newLength) {
    checkClosed();
    checkReadOnly();
    if (newLength < 0) {
      throw new IllegalArgumentException("Blob cannot be truncated to a negative length");
    }
    if (newLength > getSize()) {
      throw new IllegalArgumentException(
          "Blob size cannot be extended to a bigger size by calling truncate");
    }
    if (position > newLength) {
      throw new IllegalArgumentException(
          "Blob cannot be truncated to a size that is before the current position");
    }
    blobData.content = new ArrayList<Byte>(blobData.content.subList(0, (int) newLength));
  }

  @Override
  public long getVersion() {
    checkClosed();
    return previousVersion;
  }

  @Override
  public void write(final byte[] b, final int off, final int len) {
    checkClosed();
    checkReadOnly();
    for (int i = 0; i < len; i++) {
      if (position < getSize()) {
        blobData.content.set((int) position, b[off + i]);
      } else {
        blobData.content.add(b[off + i]);
      }
      position++;
    }
  }

}
