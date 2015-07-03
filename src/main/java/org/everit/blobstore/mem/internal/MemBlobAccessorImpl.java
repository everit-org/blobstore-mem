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

import org.everit.blobstore.api.BlobAccessor;

/**
 * Internal {@link BlobAccessor} implementation for {@link org.everit.blobstore.mem.MemBlobstore}.
 */
public class MemBlobAccessorImpl implements BlobAccessor {

  private final BlobData data;

  private long position = 0;

  private final long previousVersion;

  private final boolean readOnly;

  /**
   * Constructor.
   *
   * @param data
   *          The metadata of the <code>BLOB</code>.
   * @param previousVersion
   *          The previous version of the <code>BLOB</code>. In case of readOnly mode or new
   *          <code>BLOBs</code> this number should be the same as {@link BlobData#version}.
   * @param readOnly
   *          Whether this <code>BLOB</code> manipulation is allowed via this accessor or not.
   */
  public MemBlobAccessorImpl(final BlobData data, final long previousVersion,
      final boolean readOnly) {

    this.data = data;
    this.previousVersion = previousVersion;
    this.readOnly = readOnly;
  }

  private void checkReadOnly() {
    if (readOnly) {
      throw new IllegalStateException("BLOB cannot be modified via reader.");
    }
  }

  @Override
  public long newVersion() {
    return data.version;
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public int read(final byte[] b, final int off, final int len) {
    int n = (int) (size() - position);
    if (n == 0) {
      return -1;
    }
    if (len < n) {
      n = len;
    }

    int loff = off;
    for (int i = 0; i < n; i++) {
      b[loff] = data.content.get((int) position);
      loff++;
      position++;
    }
    return n;
  }

  @Override
  public void seek(final long pos) {
    if (pos > size() || pos < 0) {
      throw new IllegalArgumentException();
    }
    position = pos;
  }

  @Override
  public long size() {
    return data.content.size();
  }

  @Override
  public void truncate(final long newLength) {
    checkReadOnly();
    if (newLength < 0 || newLength > size() || position > newLength) {
      throw new IllegalArgumentException();
    }
    data.content = new ArrayList<Byte>(data.content.subList(0, (int) newLength));
  }

  @Override
  public long version() {
    return previousVersion;
  }

  @Override
  public void write(final byte[] b, final int off, final int len) {
    checkReadOnly();
    for (int i = 0; i < len; i++) {
      if (position < size()) {
        data.content.set((int) position, b[off + i]);
      } else {
        data.content.add(b[off + i]);
      }
      position++;
    }
  }

}
