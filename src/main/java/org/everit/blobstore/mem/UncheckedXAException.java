package org.everit.blobstore.mem;

import javax.transaction.xa.XAException;

public class UncheckedXAException extends RuntimeException {

  public UncheckedXAException(final XAException cause) {
    super(cause);
  }

}
