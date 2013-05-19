package edu.washington.escience.myriad.column;

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
    System.out.println(original.build().toString());
    System.out.println(deserialized.toString());
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
