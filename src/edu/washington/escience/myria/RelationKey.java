package edu.washington.escience.myria;

import java.io.Serializable;
import java.util.Objects;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 * This class holds the key that identifies a relation. The notation is user.program.relation.
 * 
 * @author dhalperi
 */
public final class RelationKey implements Serializable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The user who owns/creates this relation. */
  @JsonProperty
  private final String userName;
  /** The user's program that owns/creates this relation. */
  @JsonProperty
  private final String programName;
  /** The name of the relation. */
  @JsonProperty
  private final String relationName;

  /**
   * Static function to create a RelationKey object.
   * 
   * @param userName the user who owns/creates this relation.
   * @param programName the user's program that owns/creates this relation.
   * @param relationName the name of the relation.
   * @return a new RelationKey reference to the specified relation.
   */
  @JsonCreator
  public static RelationKey of(@JsonProperty("userName") final String userName,
      @JsonProperty("programName") final String programName, @JsonProperty("relationName") final String relationName) {
    return new RelationKey(userName, programName, relationName);
  }

  /** The regular expression specifying what names are valid. */
  public static final String VALID_NAME_REGEX = "^[a-zA-Z_]\\w*$";
  /** The regular expression matcher for {@link #VALID_NAME_REGEX}. */
  private static final Pattern VALID_NAME_PATTERN = Pattern.compile(VALID_NAME_REGEX);

  /**
   * Validate a potential user, program, relation name for use in a Relation Key. Valid names are given by
   * {@link #VALID_NAME_REGEX}.
   * 
   * @param name the candidate user, program, or relation name.
   * @param whichName 'user', 'program', or 'relation'.
   * @return the supplied name, if it is valid.
   * @throws IllegalArgumentException if the name does not match the regex {@link #VALID_NAME_REGEX}.
   */
  private static String checkName(final String name, final String whichName) {
    Objects.requireNonNull(name, whichName);
    Preconditions.checkArgument(VALID_NAME_PATTERN.matcher(name).matches(),
        "supplied %s '%s' does not match the valid name regex %s", whichName, name, VALID_NAME_REGEX);
    return name;
  }

  /**
   * Private constructor to create a RelationKey object.
   * 
   * @param userName the user who owns/creates this relation.
   * @param programName the user's program that owns/creates this relation.
   * @param relationName the name of the relation.
   */
  public RelationKey(final String userName, final String programName, final String relationName) {
    checkName(userName, "userName");
    checkName(programName, "programName");
    checkName(relationName, "relationName");
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
    return toString('[', '#', ']');
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
    StringBuilder sb = new StringBuilder();
    sb.append(leftEscape).append(userName).append(separate).append(programName).append(separate).append(relationName)
        .append(rightEscape);
    return sb.toString();
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
        return toString('[', '#', ']');
      case MyriaConstants.STORAGE_SYSTEM_POSTGRESQL:
      case MyriaConstants.STORAGE_SYSTEM_MONETDB:
        return toString('\"', ' ', '\"');
      case MyriaConstants.STORAGE_SYSTEM_MYSQL:
        return toString('`', ' ', '`');
      default:
        throw new IllegalArgumentException("Unsupported dbms " + dbms);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(userName, programName, relationName);
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof RelationKey)) {
      return false;
    }
    RelationKey o = (RelationKey) other;
    return Objects.equals(userName, o.userName) && Objects.equals(programName, o.programName)
        && Objects.equals(relationName, o.relationName);
  }
}
