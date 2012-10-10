package edu.washington.escience.myriad.datalog.syntax;

/**
 * Represents a Datalog type Long.
 */
public final class DatalogTypeLong extends DatalogType {
  /** Using the Singleton template, creates the instance of this object. */
  private static final DatalogTypeLong INSTANCE = new DatalogTypeLong();

  /** Inaccessible private constructor. */
  private DatalogTypeLong() {
  }

  /** @return the Datalog Long type. */
  public static DatalogTypeLong getInstance() {
    return INSTANCE;
  }

  @Override
  public String toString() {
    return "<Long>";
  }
}