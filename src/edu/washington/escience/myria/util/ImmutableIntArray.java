package edu.washington.escience.myria.util;

import java.util.Iterator;

import com.google.common.base.Preconditions;

/**
 * An immutable int array.
 * */
public class ImmutableIntArray implements Iterable<Integer> {

  /**
   * Valid tuple index wrapper. For the purpose of thread safety.
   * */
  /**
   * @param valueArray to wrap
   * */
  public ImmutableIntArray(final int[] valueArray) {
    Preconditions.checkNotNull(valueArray);
    this.valueArray = valueArray;
  }

  /** An ImmutableList<Integer> view of the indices of validTuples. */
  private final int[] valueArray;

  /**
   * @return the index'th element
   * @param index the index
   * */
  public final int get(final int index) {
    return valueArray[index];
  }

  /**
   * @return the array length
   * */
  public final int length() {
    return valueArray.length;
  }

  @Override
  public Iterator<Integer> iterator() {
    return new IndexIterator();
  }

  /**
   * iterator over the elements.
   * */
  private class IndexIterator implements Iterator<Integer> {
    /**
     * current index.
     * */
    private int i = 0;

    @Override
    public final boolean hasNext() {
      return valueArray.length > i;
    }

    @Override
    public final Integer next() {
      return valueArray[i++];
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Read only");
    }
  }
}
