package edu.washington.escience.myria.expression.evaluate;

import edu.washington.escience.myria.Schema;

/**
 * Object that carries parameters down the expression tree.
 */
public class ExpressionOperatorParameter {
  /** The input schema. */
  private final Schema schema;
  /** The schema of the state. */
  private final Schema stateSchema;
  /** The id of the worker that is running the expression. */
  private final Integer workerID;

  /**
   * Simple constructor.
   */
  public ExpressionOperatorParameter() {
    schema = null;
    stateSchema = null;
    workerID = null;
  }

  /**
   * @param schema the input schema
   */
  public ExpressionOperatorParameter(final Schema schema) {
    this.schema = schema;
    stateSchema = null;
    workerID = null;
  }

  /**
   * @param schema the input schema
   * @param stateSchema the state schema
   */
  public ExpressionOperatorParameter(final Schema schema, final Schema stateSchema) {
    this.schema = schema;
    this.stateSchema = stateSchema;
    workerID = null;
  }

  /**
   * @param schema the input schema
   * @param workerID id of the worker that is running the expression
   */
  public ExpressionOperatorParameter(final Schema schema, final int workerID) {
    this.schema = schema;
    stateSchema = null;
    this.workerID = workerID;
  }

  /**
   * @param schema the input schema
   * @param stateSchema the schema of the state
   * @param workerID id of the worker that is running the expression
   */
  public ExpressionOperatorParameter(
      final Schema schema, final Schema stateSchema, final int workerID) {
    this.schema = schema;
    this.stateSchema = stateSchema;
    this.workerID = workerID;
  }

  /**
   * @return the input schema
   */
  public Schema getSchema() {
    return schema;
  }

  /**
   * @return the schema of the state
   */
  public Schema getStateSchema() {
    return stateSchema;
  }

  /**
   * @return the id of the worker that the expression is executed on
   */
  public int getWorkerId() {
    return workerID;
  }
}
