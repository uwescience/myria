package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;

import org.junit.Test;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.column.LongColumn;
import edu.washington.escience.myria.column.builder.LongColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;

public class LongColumnTest {

  @Test
  public void testProto() {
    final LongColumnBuilder original = new LongColumnBuilder();
    original.append(1).append(2).append(5).append(11);
    final ColumnMessage serialized = original.build().serializeToProto();
    final LongColumn deserialized = LongColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertTrue(original.build().toString().equals(deserialized.toString()));
  }

  @Test
  public void testFull() {
    final LongColumnBuilder builder = new LongColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.append(i);
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final LongColumnBuilder builder = new LongColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.append(i);
    }
    builder.append(0);
    builder.build();
  }

}
