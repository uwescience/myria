package edu.washington.escience.myriad.operator;

/**
 * Simple abstract operator meant to make it easy to implement leaf operators.
 * 
 * @author dhalperi
 * 
 */
public abstract class LeafOperator extends Operator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  @Override
  public final Operator[] getChildren() {
    return null;
  }

  @Override
  public final void setChildren(final Operator[] children) {
    throw new UnsupportedOperationException();
  }
}
