package edu.washington.escience.myriad.column;

import static org.junit.Assert.assertEquals;

import java.nio.BufferOverflowException;

import org.junit.Test;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;

public class FloatColumnTest {

  @Test
  public void testProto() {
    final FloatColumnBuilder original = new FloatColumnBuilder();
    original.append(1.0f).append(2.0f).append(5.0f).append(11.0f);
    FloatColumn column = original.build();
    final ColumnMessage serialized = column.serializeToProto();
    final FloatColumn deserialized = FloatColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertEquals(column.get(0), deserialized.get(0));
    assertEquals(column.get(1), deserialized.get(1));
    assertEquals(column.get(2), deserialized.get(2));
    assertEquals(column.get(3), deserialized.get(3));
  }

  @Test
  public void testFull() {
    final FloatColumnBuilder builder = new FloatColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.append(i * 1.0f);
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final FloatColumnBuilder builder = new FloatColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.append(i * 1.0f);
    }
    builder.append(0.0f);
    builder.build();
  }

}
