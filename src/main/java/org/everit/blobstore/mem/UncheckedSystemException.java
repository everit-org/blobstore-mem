package org.everit.blobstore.mem;

import javax.transaction.SystemException;

public class UncheckedSystemException extends RuntimeException {

  private static final long serialVersionUID = 524341239283098140L;

  public UncheckedSystemException(final SystemException cause) {
    super(cause);
  }

}
