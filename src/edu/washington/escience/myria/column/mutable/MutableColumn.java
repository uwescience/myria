package edu.washington.escience.myria.column.mutable;

import edu.washington.escience.myria.column.Column;

/**
 * A mutable column of a batch of tuples.
 * 
 * @param <T> type of the objects in this column.
 * 
 */
public abstract class MutableColumn<T extends Comparable<?>> extends Column<T> implements Cloneable {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * replace the old value at index with the new value.
   * 
   * @param index the index
   * @param value the new value
   */
  public abstract void replace(int index, T value);

  /**
   * return the column representation of this mutable column. It copies data.
   * 
   * @return the column representation of this mutable column.
   */
  public abstract Column<T> toColumn();

  /**
   * clone itself by copying data.
   * 
   * @return the clone
   */
  @Override
  public abstract MutableColumn<T> clone();
}