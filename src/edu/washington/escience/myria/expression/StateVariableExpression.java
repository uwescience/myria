package edu.washington.escience.myria.expression;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.operator.StatefulApply;

/**
 * Simple expression operator that allows access to the state in {@link StatefulApply}.
 */
public class StateVariableExpression extends VariableExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * Default constructor.
   * 
   * @param columnIdx the index in the state.
   */
  public StateVariableExpression(final int columnIdx) {
    super(columnIdx);
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
}
