package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;

import org.junit.Test;

import edu.washington.escience.myria.column.LongColumn;
import edu.washington.escience.myria.column.builder.LongColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.storage.TupleBatch;

public class LongColumnTest {

  @Test
  public void testProto() {
    final LongColumnBuilder original = new LongColumnBuilder();
    original.appendLong(1).appendLong(2).appendLong(5).appendLong(11);
    final ColumnMessage serialized = original.build().serializeToProto();
    final LongColumn deserialized =
        LongColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertTrue(original.build().toString().equals(deserialized.toString()));
  }

  @Test
  public void testFull() {
    final LongColumnBuilder builder = new LongColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.appendLong(i);
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final LongColumnBuilder builder = new LongColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.appendLong(i);
    }
    builder.appendLong(0);
    builder.build();
  }
}
