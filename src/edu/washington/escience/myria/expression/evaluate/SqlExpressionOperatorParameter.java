package edu.washington.escience.myria.expression.evaluate;

import java.util.HashMap;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;

/**
 * Parameters passed to expressions.
 */
public class SqlExpressionOperatorParameter extends ExpressionOperatorParameter {
  /**
   * @param dbms the dbms
   * @param nodeID the node id
   */
  public SqlExpressionOperatorParameter(final String dbms, final int nodeID) {
    setDbms(dbms);
    setNodeID(nodeID);
  }

  /**
   * @param schemas the schemas of the input relations
   * @param dbms the database system
   * @param nodeID the node id
   */
  public SqlExpressionOperatorParameter(final HashMap<RelationKey, Schema> schemas, final String dbms, final int nodeID) {
    setSchemas(schemas);
    setDbms(dbms);
    setNodeID(nodeID);
  }
}
