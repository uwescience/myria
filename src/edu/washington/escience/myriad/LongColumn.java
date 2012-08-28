package edu.washington.escience.myriad;

import java.nio.LongBuffer;

public class LongColumn extends Column {
  LongBuffer data;

  public LongColumn() {
    this.data = LongBuffer.allocate(TupleBatch.BATCH_SIZE);
  }

  @Override
  public Object get(int row) {
    return Long.valueOf(getLong(row));
  }

  public long getLong(int index) {
    return data.get(index);
  }

  @Override
  public void put(Object value) {
    putLong((Long) value);
  }

  public void putLong(long value) {
    data.put(value);
  }

  @Override
  int size() {
    return data.position();
  }
}