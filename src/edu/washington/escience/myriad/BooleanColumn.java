package edu.washington.escience.myriad;

import java.util.BitSet;

import com.google.common.base.Preconditions;

public class BooleanColumn extends Column {
  BitSet data;
  int numBits;

  public BooleanColumn() {
    this.data = new BitSet(TupleBatch.BATCH_SIZE);
    this.numBits = 0;
  }

  public boolean getBoolean(int row) {
    Preconditions.checkElementIndex(row, numBits);
    return data.get(row);
  }

  public void putBoolean(boolean value) {
    Preconditions.checkElementIndex(numBits, TupleBatch.BATCH_SIZE);
    data.set(numBits, value);
    numBits++;
  }

  @Override
  public Object get(int row) {
    return Boolean.valueOf(getBoolean(row));
  }

  @Override
  public void put(Object value) {
    putBoolean((Boolean) value);
  }

  @Override
  int size() {
    return numBits;
  }
}