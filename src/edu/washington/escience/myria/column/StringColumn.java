package edu.washington.escience.myria.column;

import edu.washington.escience.myria.Type;

/**
 * An abstract column of String values.
 *
 */
public abstract class StringColumn extends Column<String> {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  @Override
  public final String getObject(final int row) {
    return getString(row);
  }

  @Override
  public abstract String getString(final int row);

  @Override
  public final Type getType() {
    return Type.STRING_TYPE;
  }

  @Override
  public final String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(size()).append(" elements: [");
    for (int i = 0; i < size(); ++i) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(getString(i));
    }
    sb.append(']');
    return sb.toString();
  }
}
