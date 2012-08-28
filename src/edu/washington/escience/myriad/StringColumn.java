package edu.washington.escience.myriad;

import com.google.common.base.Preconditions;

public class StringColumn extends Column {
  int startIndices[];
  int endIndices[];
  StringBuilder data;
  int numStrings;

  public StringColumn() {
    this.startIndices = new int[TupleBatch.BATCH_SIZE];
    this.endIndices = new int[TupleBatch.BATCH_SIZE];
    this.data = new StringBuilder();
    this.numStrings = 0;
  }

  public StringColumn(int averageStringSize) {
    this.startIndices = new int[TupleBatch.BATCH_SIZE];
    this.endIndices = new int[TupleBatch.BATCH_SIZE];
    this.data = new StringBuilder(averageStringSize * TupleBatch.BATCH_SIZE);
    this.numStrings = 0;
  }

  @Override
  public Object get(int row) {
    return getString(row);
  }

  public String getString(int row) {
    Preconditions.checkElementIndex(row, numStrings);
    return data.substring(startIndices[row], endIndices[row]);
  }

  @Override
  public void put(Object value) {
    putString((String) value);
  }

  public void putString(String input) {
    startIndices[numStrings] = data.length();
    data.append(input);
    endIndices[numStrings] = data.length();
    numStrings++;
  }

  @Override
  int size() {
    return numStrings;
  }
}