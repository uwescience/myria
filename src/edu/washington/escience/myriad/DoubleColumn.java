package edu.washington.escience.myriad;

import java.nio.DoubleBuffer;

public class DoubleColumn extends Column {
  DoubleBuffer data;

  public DoubleColumn() {
    this.data = DoubleBuffer.allocate(TupleBatch.BATCH_SIZE);
  }

  public double getDouble(int index) {
    return data.get(index);
  }

  public void putDouble(double value) {
    data.put(value);
  }
}