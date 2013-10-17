package edu.washington.escience.myria.expression;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;

/**
 * Represents a reference to a child field in an expression tree.
 */
public class VariableExpression extends ZeroaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /** The index in the input that this {@link VariableExpression} references. */
  @JsonProperty
  private final int columnIdx;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private VariableExpression() {
    columnIdx = -1;
  }

  /**
   * A {@link VariableExpression} that references column <code>columnIdx</code> from the input.
   * 
   * @param columnIdx the index in the input that this {@link VariableExpression} references.
   */
  public VariableExpression(final int columnIdx) {
    this.columnIdx = columnIdx;
  }

  @Override
  public Type getOutputType(final Schema schema) {
    return schema.getColumnType(columnIdx);
  }

  @Override
  public String getJavaString(final Schema schema) {
    String tName = null;
    switch (getOutputType(schema)) {
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

    // We generate a variable access into the tuple buffer.
    return new StringBuilder("tb.get").append(tName).append("(").append(columnIdx).append(", rowId)").toString();
  }

  /**
   * @return the column index of this variable.
   */
  public int getColumnIdx() {
    return columnIdx;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass().getCanonicalName(), columnIdx);
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof VariableExpression)) {
      return false;
    }
    VariableExpression otherExp = (VariableExpression) other;
    return Objects.equals(columnIdx, otherExp.columnIdx);
  }
}
