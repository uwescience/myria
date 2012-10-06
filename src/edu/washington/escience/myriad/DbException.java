package edu.washington.escience.myriad;

/** Generic database exception class. */
public class DbException extends Exception {
  /** Required for serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Standard String constructor.
   * 
   * @param s a String describing the exception.
   */
  public DbException(final String s) {
    super(s);
  }

  /**
   * Standard Throwable constructor.
   * 
   * @param e a different Throwable to be wrapped in a DbException.
   */
  public DbException(final Throwable e) {
    super(e);
  }
}