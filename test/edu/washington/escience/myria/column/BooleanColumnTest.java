package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;

import org.junit.Test;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.column.BooleanColumn;
import edu.washington.escience.myria.column.BooleanColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;

public class BooleanColumnTest {

  @Test
  public void testProto() {
    final BooleanColumnBuilder original = new BooleanColumnBuilder();
    original.append(true).append(false).append(true).append(false).append(false).append(false).append(false).append(
        false).append(true).append(false).append(false).append(false).append(false).append(false);
    final ColumnMessage serialized = original.build().serializeToProto();
    final BooleanColumn deserialized = BooleanColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertTrue(original.build().toString().equals(deserialized.toString()));
  }

  @Test
  public void testFull() {
    final BooleanColumnBuilder builder = new BooleanColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.append(true);
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final BooleanColumnBuilder builder = new BooleanColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.append(false);
    }
    builder.append(true);
    builder.build();
  }

}
