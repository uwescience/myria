package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;

import org.junit.Test;

import edu.washington.escience.myria.column.builder.DoubleColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.storage.TupleBatch;

public class DoubleColumnTest {

  @Test
  public void testProto() {
    final DoubleColumnBuilder original = new DoubleColumnBuilder();
    original.appendDouble(1).appendDouble(2).appendDouble(5).appendDouble(11);
    final ColumnMessage serialized = original.build().serializeToProto();
    final DoubleColumn deserialized =
        DoubleColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertTrue(original.build().toString().equals(deserialized.toString()));
  }

  @Test
  public void testFull() {
    final DoubleColumnBuilder builder = new DoubleColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.appendDouble(i * 1.0);
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final DoubleColumnBuilder builder = new DoubleColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.appendDouble(i * 1.0);
    }
    builder.appendDouble(0.0);
    builder.build();
  }
}
