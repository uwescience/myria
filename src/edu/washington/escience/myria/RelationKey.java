package edu.washington.escience.myria;

import java.io.Serializable;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class holds the key that identifies a relation. The notation is user.program.relation.
 * 
 * @author dhalperi
 */
public final class RelationKey implements Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The user who owns/creates this relation. */
  private final String userName;
  /** The user's program that owns/creates this relation. */
  private final String programName;
  /** The name of the relation. */
  private final String relationName;
  /** Canonical table name. Used as a lazy cache for the toString operator. */
  private String canonicalTableName;

  /**
   * Static function to create a RelationKey object.
   * 
   * @param userName the user who owns/creates this relation.
   * @param programName the user's program that owns/creates this relation.
   * @param relationName the name of the relation.
   * @return a new RelationKey reference to the specified relation.
   */
  public static RelationKey of(final String userName, final String programName, final String relationName) {
    return new RelationKey(userName, programName, relationName);
  }

  /**
   * Private constructor to create a RelationKey object.
   * 
   * @param userName the user who owns/creates this relation.
   * @param programName the user's program that owns/creates this relation.
   * @param relationName the name of the relation.
   */
  public RelationKey(@JsonProperty("user_name") final String userName,
      @JsonProperty("program_name") final String programName, @JsonProperty("relation_name") final String relationName) {
    Objects.requireNonNull(userName);
    Objects.requireNonNull(programName);
    Objects.requireNonNull(relationName);
    this.userName = userName;
    this.programName = programName;
    this.relationName = relationName;
  }

  /**
   * @return the name of this relation.
   */
  public String getRelationName() {
    return relationName;
  }

  /**
   * @return the name of the program containing this relation.
   */
  public String getProgramName() {
    return programName;
  }

  /**
   * @return the name of the user who owns the program containing this relation.
   */
  public String getUserName() {
    return userName;
  }

  @SuppressWarnings("unused")
  @Override
  @Deprecated
  public String toString() {
    if (true) {
      throw new UnsupportedOperationException("Use toString(dbms)!");
    }
    if (canonicalTableName != null) {
      return canonicalTableName;
    }
    canonicalTableName = toString('[', '#', ']');
    return canonicalTableName;
  }

  /**
   * Helper function for computing strings of different types.
   * 
   * @param leftEscape the left escape character, e.g., '['.
   * @param separate the separating character, e.g., '#'.
   * @param rightEscape the right escape character, e.g., ']'.
   * @return [user#program#relation].
   */
  private String toString(final char leftEscape, final char separate, final char rightEscape) {
    if (canonicalTableName != null) {
      return canonicalTableName;
    }
    StringBuilder sb = new StringBuilder();
    sb.append(leftEscape).append(userName).append(separate).append(programName).append(separate).append(relationName)
        .append(rightEscape);
    canonicalTableName = sb.toString();
    return canonicalTableName;
  }

  /**
   * Helper function for computing strings of different types.
   * 
   * @param dbms the DBMS, e.g., "mysql".
   * @return [user#program#relation].
   */
  public String toString(final String dbms) {
    switch (dbms) {
      case MyriaConstants.STORAGE_SYSTEM_SQLITE:
      case MyriaConstants.STORAGE_SYSTEM_MYSQL:
        return toString('[', '#', ']');
      case MyriaConstants.STORAGE_SYSTEM_MONETDB:
        return toString('\"', ' ', '\"');
      default:
        throw new IllegalArgumentException("Unsupported dbms " + dbms);
    }
  }
}
