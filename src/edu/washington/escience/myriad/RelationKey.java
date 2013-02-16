package edu.washington.escience.myriad;

import java.io.Serializable;
import java.util.Objects;

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
   * Static function to create a RelationName object.
   * 
   * @param userName the user who owns/creates this relation.
   * @param programName the user's program that owns/creates this relation.
   * @param relationName the name of the relation.
   * @return a new RelationName reference to the specified relation.
   */
  public static RelationKey of(final String userName, final String programName, final String relationName) {
    return new RelationKey(userName, programName, relationName);
  }

  /**
   * Private constructor to create a RelationName object.
   * 
   * @param userName the user who owns/creates this relation.
   * @param programName the user's program that owns/creates this relation.
   * @param relationName the name of the relation.
   */
  private RelationKey(final String userName, final String programName, final String relationName) {
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

  @Override
  public String toString() {
    if (canonicalTableName != null) {
      return canonicalTableName;
    }
    StringBuilder sb = new StringBuilder();
    sb.append('[').append(userName).append('#').append(programName).append('#').append(relationName).append(']');
    canonicalTableName = sb.toString();
    return canonicalTableName;
  }
}