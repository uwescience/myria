package edu.washington.escience.myria.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
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
  private final int columnIdx;

  /**
   * A {@link VariableExpression} that references column <code>columnIdx</code> from the input.
   * 
   * @param columnIdx the index in the input that this {@link VariableExpression} references.
   */
  @JsonCreator
  public VariableExpression(@JsonProperty("column_idx") final int columnIdx) {
    this.columnIdx = columnIdx;
  }

  @Override
  public Type getOutputType(final Schema schema) {
    return schema.getColumnType(columnIdx);
  }

  @Override
  public String getJavaString(final Schema schema) {
    String tName;
    switch (getOutputType(schema)) {
      case DOUBLE_TYPE:
        tName = "Double";
        break;

      case LONG_TYPE:
        tName = "Long";
        break;

      case INT_TYPE:
        tName = "Int";
        break;

      case FLOAT_TYPE:
        tName = "Float";
        break;

      case BOOLEAN_TYPE:
        tName = "Boolean";
        break;

      case STRING_TYPE:
        tName = "String";
        break;

      case DATETIME_TYPE:
        tName = "DateTime";
        break;

      default:
        // TODO: better exception
        tName = getOutputType(schema).name();
    }

    // We generate a variable access into the tuple buffer.
    return new StringBuilder("tb.get").append(tName).append("(").append(columnIdx).append(", rowId)").toString();
  }

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof VariableExpression)) {
      return false;
    }
    return ((VariableExpression) other).columnIdx == columnIdx;
  }

  @Override
  public int hashCode() {
    return columnIdx;
  }

  /**
   * @return the column index of this variable.
   */
  public int getColumnIdx() {
    return columnIdx;
  }
}
