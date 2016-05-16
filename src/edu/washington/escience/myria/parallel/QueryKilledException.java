package edu.washington.escience.myria.parallel;

/**
 * The exception for describing the killed cause of a query failure. A query failure cause by killed is completely
 * different from a query failure cause by other Exception. The former failure is intended while the latter is
 * surprising.
 * */
public class QueryKilledException extends InterruptedException {

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  /**
   * Default constructor.
   * */
  public QueryKilledException() {}

  /**
   * @param s the message describing the detail of this Exception.
   * */
  public QueryKilledException(final String s) {
    super(s);
  }
}
