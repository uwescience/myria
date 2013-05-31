package edu.washington.escience.myriad.column;

import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;

import org.junit.Test;

import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;

public class IntColumnTest {

  @Test
  public void testProto() {
    final IntColumnBuilder original = new IntColumnBuilder();
    original.append(1).append(2).append(5).append(11);
    final ColumnMessage serialized = original.build().serializeToProto();
    final IntColumn deserialized = IntColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertTrue(original.build().toString().equals(deserialized.toString()));
  }

  @Test
  public void testFull() {
    final IntColumnBuilder builder = new IntColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.append(i);
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final IntColumnBuilder builder = new IntColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.append(i);
    }
    builder.append(TupleBatch.BATCH_SIZE);
    builder.build();
  }

}
