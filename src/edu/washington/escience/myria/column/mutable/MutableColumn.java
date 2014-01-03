package edu.washington.escience.myria.column.mutable;

import edu.washington.escience.myria.column.Column;

/**
 * A mutable column of a batch of tuples.
 * 
 * @param <T> type of the objects in this column.
 * 
 */
public interface MutableColumn<T extends Comparable<?>> extends Column<T>, Cloneable {

  /**
   * replace the old value at index with the new value.
   * 
   * @param index the index
   * @param value the new value
   */
  void replace(int index, T value);

  /**
   * return the column representation of this mutable column. It copies data.
   * 
   * @return the column representation of this mutable column.
   */
  Column<T> toColumn();

  /**
   * clone itself by copying data.
   * 
   * @return the clone
   */
  MutableColumn<T> clone();
}