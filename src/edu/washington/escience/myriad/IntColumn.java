package edu.washington.escience.myriad;

import java.nio.IntBuffer;

public class IntColumn extends Column {
  IntBuffer data;

  public IntColumn() {
    this.data = IntBuffer.allocate(TupleBatch.BATCH_SIZE);
  }

  public int getInt(int index) {
    return data.get(index);
  }

  public void putInt(int value) {
    data.put(value);
  }
}