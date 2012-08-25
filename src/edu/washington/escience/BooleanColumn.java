package edu.washington.escience;

import java.util.BitSet;

import com.google.common.base.Preconditions;

public class BooleanColumn extends Column {
  BitSet data;
  int numBits;

  public BooleanColumn() {
    this.data = new BitSet(TupleBatch.BATCH_SIZE);
    this.numBits = 0;
  }

  public boolean getBoolean(int index) {
    Preconditions.checkElementIndex(index, numBits);
    return data.get(index);
  }

  public void putBoolean(boolean value) {
    Preconditions.checkElementIndex(numBits, TupleBatch.BATCH_SIZE);
    data.set(numBits, value);
    numBits++;
  }
}