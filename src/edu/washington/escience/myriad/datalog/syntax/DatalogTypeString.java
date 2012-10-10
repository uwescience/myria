package edu.washington.escience.myriad.datalog.syntax;

/**
 * Represents a Datalog type String.
 */
public final class DatalogTypeString extends DatalogType {
  /** Using the Singleton template, creates the instance of this object. */
  private static final DatalogTypeString INSTANCE = new DatalogTypeString();

  /** @return the Datalog String type. */
  public static DatalogTypeString getInstance() {
    return INSTANCE;
  }

  /** Inaccessible private constructor. */
  private DatalogTypeString() {
  }

  @Override
  public String toString() {
    return "<String>";
  }
}