package edu.washington.escience.myriad;

import java.nio.IntBuffer;

import com.google.common.base.Preconditions;

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

  @Override
  public Object get(int row) {
    Preconditions.checkElementIndex(row, data.position());
    return Integer.valueOf(data.get(row));
  }

  @Override
  public void put(Object value) {
    putInt((Integer) value);
  }

  @Override
  int size() {
    return data.position();
  }
}