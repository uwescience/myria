package edu.washington.escience.myria.column;

/**
 * A mutable column of a batch of tuples.
 * 
 * @param <T> type of the objects in this column.
 * 
 */
public interface MutableColumn<T extends Comparable<?>> extends Column<T> {

  /**
   * replace the old value at index with the new value.
   * 
   * @param index the index
   * @param value the new value
   */
  void replace(int index, T value);
}