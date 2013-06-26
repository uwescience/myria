package edu.washington.escience.myriad.util;

import edu.washington.escience.myriad.DbException;

/**
 * Exception denoting a injected failure.
 * */
public class InjectedFailureException extends DbException {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  /**
   * @param message the message describing the exception.
   * */
  public InjectedFailureException(final String message) {
    super(message);
  }

  /**
   * @param e the cause of this Exception.
   * */
  public InjectedFailureException(final Throwable e) {
    super(e);
  }

}
