package edu.washington.escience.myriad.column;

import com.google.common.base.Preconditions;

import edu.washington.escience.myriad.TupleBatch;

/**
 * A column of String values.
 * 
 * @author dhalperi
 * 
 */
public final class StringColumn implements Column {
  /**
   * The positions of the starts of each String in this column. Used to pack variable-length Strings
   * and yet still have fast lookup.
   */
  private final int[] startIndices;
  /**
   * The positions of the ends of each String in this column. Used to pack variable-length Strings
   * and yet still have fast lookup.
   */
  private final int[] endIndices;
  /** Contains the packed character data. */
  private final StringBuilder data;
  /** Number of elements in this column. */
  private int numStrings;

  /** Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements. */
  public StringColumn() {
    this.startIndices = new int[TupleBatch.BATCH_SIZE];
    this.endIndices = new int[TupleBatch.BATCH_SIZE];
    this.data = new StringBuilder();
    this.numStrings = 0;
  }

  /**
   * Constructs an empty column that can hold up to TupleBatch.BATCH_SIZE elements, but uses the
   * averageStringSize to seed the initial size of the internal buffer that stores the
   * variable-length Strings.
   * 
   * @param averageStringSize expected average size of the Strings that will be stored in this
   *          column.
   */
  public StringColumn(final int averageStringSize) {
    this.startIndices = new int[TupleBatch.BATCH_SIZE];
    this.endIndices = new int[TupleBatch.BATCH_SIZE];
    this.data = new StringBuilder(averageStringSize * TupleBatch.BATCH_SIZE);
    this.numStrings = 0;
  }

  @Override
  public Object get(final int row) {
    return getString(row);
  }

  /**
   * Returns the element at the specified row in this column.
   * 
   * @param row row of element to return.
   * @return the element at the specified row in this column.
   */
  public String getString(final int row) {
    Preconditions.checkElementIndex(row, numStrings);
    return data.substring(startIndices[row], endIndices[row]);
  }

  @Override
  public void put(final Object value) {
    putString((String) value);
  }

  /**
   * Inserts the specified element at end of this column.
   * 
   * @param value element to be inserted.
   */
  public void putString(final String value) {
    startIndices[numStrings] = data.length();
    data.append(value);
    endIndices[numStrings] = data.length();
    numStrings++;
  }

  @Override
  public int size() {
    return numStrings;
  }
}