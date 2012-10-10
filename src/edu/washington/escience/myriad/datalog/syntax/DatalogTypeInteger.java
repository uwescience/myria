package edu.washington.escience.myriad.datalog.syntax;

/**
 * Represents a Datalog type Integer.
 */
public final class DatalogTypeInteger extends DatalogType {

  /** Using the Singleton template, creates the instance of this object. */
  private static final DatalogTypeInteger INSTANCE = new DatalogTypeInteger();

  /** Inaccessible private constructor. */
  private DatalogTypeInteger() {
  }

  /** @return the Datalog Integer type. */
  public static DatalogTypeInteger getInstance() {
    return INSTANCE;
  }

  @Override
  public String toString() {
    return "<Integer>";
  }
}