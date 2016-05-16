package edu.washington.escience.myria.operator;

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
   */
  private Operator[] children;

  /**
   * @param children the children of this operator
   */
  public NAryOperator(final Operator[] children) {
    if (children != null) {
      setChildren(children);
    } else {
      this.children = null;
    }
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
  public final void setChildren(final Operator[] children) {
    this.children = children;
  }

  /**
   * @return number of children
   */
  protected int getNumChildren() {
    return children.length;
  }
}
