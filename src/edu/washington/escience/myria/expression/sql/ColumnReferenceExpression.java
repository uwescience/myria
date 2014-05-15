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

/**
 * Represents a reference to a variable in a base relation. Use this in SQL expressions.
 */
public class ColumnReferenceExpression extends ZeroaryExpression {
  /***/
  private static final long serialVersionUID = 1L;

  /** The relation for the variable. */
  @JsonProperty
  private final RelationKey relationKey;

  /**
   * @return the relation
   */
  public RelationKey getRelation() {
    return relationKey;
  }

  /** The index in the input that is referenced. */
  @JsonProperty
  private final int columnIdx;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private ColumnReferenceExpression() {
    super();
    relationKey = null;
    columnIdx = -1;
  }

  /**
   * @param relation the relation for the variable
   * @param columnIdx the column index of the variable
   */
  public ColumnReferenceExpression(final RelationKey relation, final int columnIdx) {
    this.relationKey = relation;
    this.columnIdx = columnIdx;
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    return parameters.getSchema(relationKey).getColumnType(columnIdx);
  }

  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    throw new UnsupportedOperationException("Use variable expression.");
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass().getCanonicalName(), relationKey, columnIdx);
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof ColumnReferenceExpression)) {
      return false;
    }
    ColumnReferenceExpression otherExp = (ColumnReferenceExpression) other;
    return Objects.equals(relationKey, otherExp.relationKey) && columnIdx == otherExp.columnIdx;
  }

  @Override
  public String getSqlString(final ExpressionOperatorParameter params) {
    final Schema schema = params.getSchemas().get(relationKey);
    return new StringBuilder(params.getAlias(relationKey)).append(".").append(
        StringEscapeUtils.escapeSql(schema.getColumnName(columnIdx))).toString();
  }
}
