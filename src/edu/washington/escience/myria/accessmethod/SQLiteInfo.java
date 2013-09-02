package edu.washington.escience.myria.accessmethod;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import edu.washington.escience.myria.MyriaConstants;

/**
 * Holds the info for a SQLite Connection.
 * 
 * @author dhalperi
 * 
 */
public final class SQLiteInfo extends ConnectionInfo implements Serializable {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The database to connect to. */
  private final String databaseFilename;

  /**
   * Private constructor.
   * 
   * @param databaseFilename the file containing the database.
   */
  private SQLiteInfo(final String databaseFilename) {
    this.databaseFilename = databaseFilename;
  }

  /**
   * Creates a new SQLiteInfo object.
   * 
   * @param dbms a string, which must equal MyriaConstants.STORAGE_SYSTEM_SQLITE.
   * @param databaseFilename the file containing the database.
   * @return a new SQLiteInfo containing this information.
   */
  @JsonCreator
  public static SQLiteInfo of(@JsonProperty("dbms") final String dbms,
      @JsonProperty("database_filename") final String databaseFilename) {
    Preconditions.checkArgument(dbms.equals(MyriaConstants.STORAGE_SYSTEM_SQLITE), "The dbms parameter must equal "
        + MyriaConstants.STORAGE_SYSTEM_SQLITE);
    return new SQLiteInfo(databaseFilename);
  }

  /**
   * Creates a new SQLiteInfo object.
   * 
   * @param databaseFilename the file containing the database.
   * @return a new SQLiteInfo containing this information.
   */
  public static SQLiteInfo of(final String databaseFilename) {
    return new SQLiteInfo(databaseFilename);
  }

  /**
   * @return the file containing the database.
   */
  public String getDatabaseFilename() {
    return databaseFilename;
  }

  @Override
  public String getDbms() {
    return MyriaConstants.STORAGE_SYSTEM_SQLITE;
  }
}