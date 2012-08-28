package edu.washington.escience.myriad;

import java.nio.DoubleBuffer;

import com.google.common.base.Preconditions;

public class DoubleColumn extends Column {
  DoubleBuffer data;

  public DoubleColumn() {
    this.data = DoubleBuffer.allocate(TupleBatch.BATCH_SIZE);
  }

  public double getDouble(int row) {
    Preconditions.checkElementIndex(row, data.position());
    return data.get(row);
  }

  public void putDouble(double value) {
    Preconditions.checkElementIndex(data.position(), data.capacity());
    data.put(value);
  }
}