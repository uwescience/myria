package edu.washington.escience.myriad;

import java.nio.FloatBuffer;

public class FloatColumn extends Column {
  FloatBuffer data;

  public FloatColumn() {
    this.data = FloatBuffer.allocate(TupleBatch.BATCH_SIZE);
  }

  public float getFloat(int index) {
    return data.get(index);
  }

  public void putFloat(float value) {
    data.put(value);
  }
}