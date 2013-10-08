package edu.washington.escience.myria.operator;

import edu.washington.escience.myria.Schema;

/**
 * 
 * @author dominik
 * 
 */
public abstract class NAryOperator extends Operator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The children of the n-ary operator.
   * */
  private Operator[] children;

  /**
   * Default, empty constructor.
   */
  public NAryOperator() {
    super();
  }

  @Override
  public Operator[] getChildren() {
    return children;
  }

  /**
   * Return a child.
   * 
   * @param childIdx the index of the child to return
   * @return the child
   */
  public Operator getChild(final int childIdx) {
    return children[childIdx];
  }

  @Override
  public void setChildren(final Operator[] children) {
    this.children = children;
  }

  @Override
  public Schema getSchema() {
    return children[0].getSchema();
  }

  /**
   * @return number of children
   */
  protected int getNumChildren() {
    return children.length;
  }

}