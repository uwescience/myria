package edu.washington.escience.myria.operator;

import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.Expressions;

/**
 * Utilities to create Apply operators.
 */
public final class Applys {
  /** Utility class cannot be constructed. */
  private Applys() {}

  /**
   * Construct an Apply that selects the specified columns from the child operator.
   *
   * @param child the input operator
   * @param columns the columns to be retained in the output
   * @return an Apply that selects the specified columns from the child operator
   */
  public static Apply columnSelect(final Operator child, final int... columns) {
    Preconditions.checkNotNull(child, "child");
    Preconditions.checkNotNull(columns, "columns");
    Preconditions.checkArgument(columns.length > 0, "columns must not be empty");
    List<Expression> expr = Lists.newLinkedList();
    for (int i : columns) {
      expr.add(Expressions.columnSelect(child, i));
    }
    return new Apply(child, expr);
  }
}
