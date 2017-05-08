/**
 *
 */
package edu.washington.escience.myria.operator;

/**
 * A temporary relation that is inserted into the database.
 */
public abstract class AbstractDbInsert extends RootOperator implements DbWriter {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Construct this abstract database insert operator to insert tuples from its child into the database.
   *
   * @param child the source of tuples.
   */
  public AbstractDbInsert(final Operator child) {
    super(child);
  }
}
