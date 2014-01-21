package edu.washington.escience.myria.expression;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.StatefulApply;

/**
 * Simple expression operator that allows access to fields of the state in {@link StatefulApply}.
 */
public class StateExpression extends ZeroaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /** The index in the input that is referenced. */
  @JsonProperty
  private final int columnIdx;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private StateExpression() {
    columnIdx = -1;
  }

  /**
   * Default constructor.
   * 
   * @param columnIdx the index in the state.
   */
  public StateExpression(final int columnIdx) {
    this.columnIdx = columnIdx;
  }

  @Override
  public Type getOutputType(final Schema schema, final Schema stateSchema) {
    return stateSchema.getColumnType(getColumnIdx());
  }

  @Override
  public String getJavaString(final Schema schema, final Schema stateSchema) {
    String tName = null;
    switch (getOutputType(schema, stateSchema)) {
      case INT_TYPE:
        tName = "Int";
        break;

      case FLOAT_TYPE:
        tName = "Float";
        break;

      case DOUBLE_TYPE:
        tName = "Double";
        break;

      case BOOLEAN_TYPE:
        tName = "Boolean";
        break;

      case STRING_TYPE:
        tName = "String";
        break;

      case LONG_TYPE:
        tName = "Long";
        break;

      case DATETIME_TYPE:
        tName = "DateTime";
        break;
    }

    // We generate a variable access into the state tuple.
    return new StringBuilder("state.get").append(tName).append("(").append(getColumnIdx()).append(", 0)").toString();
  }

  /**
   * @return the column index of this variable.
   */
  public int getColumnIdx() {
    return columnIdx;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass().getCanonicalName(), getColumnIdx());
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof StateExpression)) {
      return false;
    }
    StateExpression otherExp = (StateExpression) other;
    return Objects.equals(getColumnIdx(), otherExp.getColumnIdx());
  }
}
