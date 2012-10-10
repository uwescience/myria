package edu.washington.escience.myriad.datalog.syntax;

/**
 * Represents a Datalog type Unknown.
 */
public final class DatalogTypeUnknown extends DatalogType {
  /** Using the Singleton template, creates the instance of this object. */
  private static final DatalogTypeUnknown INSTANCE = new DatalogTypeUnknown();

  /** Inaccessible private constructor. */
  private DatalogTypeUnknown() {
  }

  /** @return the Datalog Unknown type. */
  public static DatalogTypeUnknown getInstance() {
    return INSTANCE;
  }

  @Override
  public String toString() {
    return "<String>";
  }
}