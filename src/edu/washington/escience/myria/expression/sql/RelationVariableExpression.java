package edu.washington.escience.myria.expression.sql;

import java.util.Objects;

import org.apache.commons.lang.StringEscapeUtils;

import scala.collection.mutable.StringBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.ZeroaryExpression;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;
import edu.washington.escience.myria.expression.evaluate.SqlExpressionOperatorParameter;

/**
 * Represents a reference to a variable in a base relation. Use this in SQL expressions.
 */
public class RelationVariableExpression extends ZeroaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /** The relation for the variable. */
  @JsonProperty
  private final RelationKey relation;

  /** The index in the input that is referenced. */
  @JsonProperty
  private final int columnIdx;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private RelationVariableExpression() {
    this(new RelationKey("", "", ""), -1);
  }

  /**
   * @param relation the relation for the variable
   * @param columnIdx the column index of the variable
   */
  public RelationVariableExpression(final RelationKey relation, final int columnIdx) {
    this.relation = relation;
    this.columnIdx = columnIdx;
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    throw new UnsupportedOperationException("Use variable expression.");
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass().getCanonicalName(), relation, columnIdx);
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof RelationVariableExpression)) {
      return false;
    }
    RelationVariableExpression otherExp = (RelationVariableExpression) other;
    return Objects.equals(relation, otherExp.relation) && columnIdx == otherExp.columnIdx;
  }

  @Override
  public String getSqlString(final SqlExpressionOperatorParameter params) {
    final Schema schema = params.getSchemas().get(relation);
    return new StringBuilder(params.getAlias(relation)).append(".").append(
        StringEscapeUtils.escapeSql(schema.getColumnName(columnIdx))).toString();
  }
}
