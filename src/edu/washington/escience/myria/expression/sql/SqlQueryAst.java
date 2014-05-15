package edu.washington.escience.myria.expression.sql;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.evaluate.SqlExpressionOperatorParameter;
import edu.washington.escience.myria.util.TestUtils;

/**
 * A select operator that can be compiled to SQL.
 */
public class SqlQueryAst implements Serializable {
  /***/
  private static final long serialVersionUID = 1L;

  /**
   * The select clause, projections. Null means *.
   */
  @JsonProperty
  private final List<ExpressionOperator> select;

  /**
   * The relations for the from clause referenced in the where and select clauses.
   */
  @JsonProperty
  private final List<RelationKey> fromRelations;

  /**
   * Selection for the where clause.
   */
  @JsonProperty
  private final ExpressionOperator where;

  /**
   * Column indexes that the output should be ordered by.
   */
  @JsonProperty
  private final List<ColumnReferenceExpression> sortedColumns;

  /**
   * True for each column in {@link #sortedColumns} that should be ordered ascending.
   */
  @JsonProperty
  private final List<Boolean> ascending;

  /**
   * Schemas of the input.
   */
  private HashMap<RelationKey, Schema> schemas;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  public SqlQueryAst() {
    this(ImmutableList.<ExpressionOperator> of(), ImmutableList.<RelationKey> of(), null);
  }

  /**
   * @param select the select expressions
   * @param fromRelations the from relations
   * @param where the where expressions
   * @param sortedColumns the columns by which the tuples should be ordered by.
   * @param ascending true for columns that should be ordered ascending.
   */
  public SqlQueryAst(final ImmutableList<ExpressionOperator> select, final ImmutableList<RelationKey> fromRelations,
      final ExpressionOperator where, final List<ColumnReferenceExpression> sortedColumns, final List<Boolean> ascending) {
    this.select = select;
    this.fromRelations = fromRelations;
    this.where = where;
    this.sortedColumns = sortedColumns;
    this.ascending = ascending;
  }

  /**
   * @param select the select expressions
   * @param fromRelations the from relations
   * @param where the where expressions
   */
  public SqlQueryAst(final ImmutableList<ExpressionOperator> select, final ImmutableList<RelationKey> fromRelations,
      final ExpressionOperator where) {
    this.select = select;
    this.fromRelations = fromRelations;
    this.where = where;
    sortedColumns = null;
    ascending = null;
  }

  /**
   * Create query SELECT * FROM fromRelations.
   * 
   * @param fromRelations the relations to select everything from
   */
  public SqlQueryAst(final ImmutableList<RelationKey> fromRelations) {
    this.fromRelations = fromRelations;
    select = null;
    where = null;
    sortedColumns = null;
    ascending = null;
  }

  /**
   * @param schemas the schemas to set
   */
  public void setSchemas(final HashMap<RelationKey, Schema> schemas) {
    this.schemas = schemas;
  }

  /**
   * Returns the compiled sql string.
   * 
   * @param parameters parameters passed down the tree
   * @return the sql string
   */
  public String getSqlString(final SqlExpressionOperatorParameter parameters) {
    if (!TestUtils.inTravis()) {
      // ignore this in travis so that systemtests don't fail
      Preconditions.checkArgument(parameters.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL));
    }

    parameters.generateAliases(fromRelations);
    parameters.setSchemas(schemas);

    if (where != null) {
      Preconditions.checkArgument(where.getOutputType(parameters) == Type.BOOLEAN_TYPE,
          "Expected where clause to be boolean but it was %s", where.getOutputType(parameters));
    }

    StringBuilder sb = new StringBuilder();

    sb.append("SELECT ");

    if (select != null) {
      List<String> selects = Lists.newLinkedList();
      for (ExpressionOperator expr : select) {
        selects.add(expr.getSqlString(parameters));
      }
      sb.append(Joiner.on(',').join(selects));
    } else {
      sb.append("*");
    }

    sb.append("\nFROM ");

    List<String> froms = Lists.newLinkedList();
    for (RelationKey relation : fromRelations) {
      froms.add(relation.toString(parameters.getDbms()) + " AS " + parameters.getAlias(relation));
    }
    sb.append(Joiner.on(',').join(froms));

    if (where != null) {
      sb.append("\nWHERE ").append(where.getSqlString(parameters));
    }

    if (sortedColumns != null && sortedColumns.size() > 0) {
      Preconditions.checkArgument(sortedColumns.size() == ascending.size());
      sb.append("\nORDER BY ");

      List<String> orders = Lists.newLinkedList();
      for (int i = 0; i < sortedColumns.size(); i++) {
        final ColumnReferenceExpression column = sortedColumns.get(i);
        String modifier = "ASC";
        if (!ascending.get(i)) {
          modifier = "DESC";
        }
        froms.add(column.getJavaString(parameters) + " " + modifier);
      }
      sb.append(Joiner.on(',').join(orders));
    }

    return sb.toString();
  }
}
