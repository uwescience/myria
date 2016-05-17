package edu.washington.escience.myria.operator;

/**
 * Simple abstract operator meant to make it easy to implement leaf operators.
 *
 *
 */
public abstract class LeafOperator extends Operator {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  @Override
  public final Operator[] getChildren() {
    return NO_CHILDREN;
  }

  @Override
  public final void setChildren(final Operator[] children) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void checkEOSAndEOI() {
    // for reading static files, e.g. scan, there is no EOI
    setEOS();
  }
}
