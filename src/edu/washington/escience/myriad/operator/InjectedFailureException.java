package edu.washington.escience.myriad.operator;

import edu.washington.escience.myriad.DbException;

public class InjectedFailureException extends DbException {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public InjectedFailureException(String s) {
    super(s);
    // TODO Auto-generated constructor stub
  }

  public InjectedFailureException(Throwable e) {
    super(e);
    // TODO Auto-generated constructor stub
  }

}
