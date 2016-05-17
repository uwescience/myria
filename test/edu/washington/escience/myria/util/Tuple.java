package edu.washington.escience.myria.util;

@SuppressWarnings({"rawtypes", "unchecked"})
public class Tuple implements Comparable<Tuple> {
  Comparable[] values;

  public Tuple(final int numFields) {
    values = new Comparable[numFields];
  }

  @Override
  public int compareTo(final Tuple o) {
    if (o == null || o.values.length != values.length) {
      throw new IllegalArgumentException("invalid tuple");
    }
    for (int i = 0; i < values.length; i++) {
      final int sub = values[i].compareTo(o.values[i]);
      if (sub != 0) {
        return sub;
      }
    }
    return 0;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Tuple)) {
      return false;
    }
    return compareTo((Tuple) o) == 0;
  }

  public Object get(final int i) {
    return values[i];
  }

  @Override
  public int hashCode() {
    int h = 1;
    for (final Comparable o : values) {
      h = 31 * h + o.hashCode();
    }
    return h;
  }

  public int numFields() {
    return values.length;
  }

  public void set(final int i, final Comparable v) {
    values[i] = v;
  }

  public void setAll(final int start, final Tuple other) {
    System.arraycopy(other.values, 0, values, start, other.values.length);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("(");
    for (int i = 0; i < values.length - 1; i++) {
      final Comparable<?> v = values[i];
      sb.append(v + ", ");
    }
    sb.append(values[values.length - 1] + ")");
    return sb.toString();
  }
}
