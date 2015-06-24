package org.everit.blobstore.mem;

import javax.transaction.RollbackException;

public class UncheckedRollbackException extends RuntimeException {

  private static final long serialVersionUID = -2623573610120769845L;

  public UncheckedRollbackException(final RollbackException cause) {
    super(cause);
  }

}
