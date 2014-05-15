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
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.evaluate.SqlExpressionOperatorParameter;

/**
 * A select operator that can be compiled to SQL. A SQL ast.
 */
public class SelectOperator implements Serializable {
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
   * Schemas of the input.
   */
  private HashMap<RelationKey, Schema> schemas;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  public SelectOperator() {
    this(ImmutableList.<ExpressionOperator> of(), ImmutableList.<RelationKey> of(), null);
  }

  /**
   * Default constructor.
   * 
   * @param select the select expressions
   * @param fromRelations the from relations
   * @param where the where expressions
   */
  public SelectOperator(final ImmutableList<ExpressionOperator> select, final ImmutableList<RelationKey> fromRelations,
      final ExpressionOperator where) {
    this.select = select;
    this.fromRelations = fromRelations;
    this.where = where;
  }

  /**
   * Create query SELECT * FROM fromRelations.
   * 
   * @param fromRelations the relations to select everything from
   */
  public SelectOperator(final ImmutableList<RelationKey> fromRelations) {
    select = null;
    this.fromRelations = fromRelations;
    where = null;
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
    Preconditions.checkArgument(parameters.getDbms().equals(MyriaConstants.STORAGE_SYSTEM_POSTGRESQL));

    // TODO: make this work
    // Preconditions.checkArgument(where.getOutputType(parameters) == Type.BOOLEAN_TYPE);

    StringBuilder sb = new StringBuilder();
    parameters.generateAliases(fromRelations);
    parameters.setSchemas(schemas);

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

    return sb.toString();
  }
}
