package edu.washington.escience.myria.expression.evaluate;

import edu.washington.escience.myria.Schema;

/**
 * Parameters passed down expression tree for java code egneration.
 */
public class JavaExpressionOperatorParameter extends ExpressionOperatorParameter {
  /**
   * Empty constructor.
   */
  public JavaExpressionOperatorParameter() {
  }

  /**
   * @param schema the input schema
   */
  public JavaExpressionOperatorParameter(final Schema schema) {
    setSchema(schema);
  }

  /**
   * @param schema the input schema
   * @param stateSchema the state schema
   */
  public JavaExpressionOperatorParameter(final Schema schema, final Schema stateSchema) {
    setSchema(schema);
    setStateSchema(stateSchema);
  }

  /**
   * @param schema the input schema
   * @param workerID id of the worker that is running the expression
   */
  public JavaExpressionOperatorParameter(final Schema schema, final int workerID) {
    setSchema(schema);
    setNodeID(workerID);
  }

  /**
   * @param schema the input schema
   * @param stateSchema the schema of the state
   * @param workerID id of the worker that is running the expression
   */
  public JavaExpressionOperatorParameter(final Schema schema, final Schema stateSchema, final int workerID) {
    setSchema(stateSchema);
    setStateSchema(stateSchema);
    setNodeID(workerID);
  }
}
