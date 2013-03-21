package edu.washington.escience.myriad.faulttolerance;

import edu.washington.escience.myriad.DbException;

/**
 * The Exception denotes a simulated failure is triggered.
 * */
public class InjectedFailureException extends DbException {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  /**
   * @param s the message.
   * */
  public InjectedFailureException(final String s) {
    super(s);
  }

  /**
   * @param e the cause error.
   * */
  public InjectedFailureException(final Throwable e) {
    super(e);
  }

}
