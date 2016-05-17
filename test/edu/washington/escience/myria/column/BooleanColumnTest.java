package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;

import org.junit.Test;

import edu.washington.escience.myria.column.BooleanColumn;
import edu.washington.escience.myria.column.builder.BooleanColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.storage.TupleBatch;

public class BooleanColumnTest {

  @Test
  public void testProto() {
    final BooleanColumnBuilder original = new BooleanColumnBuilder();
    original
        .appendBoolean(true)
        .appendBoolean(false)
        .appendBoolean(true)
        .appendBoolean(false)
        .appendBoolean(false)
        .appendBoolean(false)
        .appendBoolean(false)
        .appendBoolean(false)
        .appendBoolean(true)
        .appendBoolean(false)
        .appendBoolean(false)
        .appendBoolean(false)
        .appendBoolean(false)
        .appendBoolean(false);
    final ColumnMessage serialized = original.build().serializeToProto();
    final BooleanColumn deserialized =
        BooleanColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertTrue(original.build().toString().equals(deserialized.toString()));
  }

  @Test
  public void testFull() {
    final BooleanColumnBuilder builder = new BooleanColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.appendBoolean(true);
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final BooleanColumnBuilder builder = new BooleanColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.appendBoolean(false);
    }
    builder.appendBoolean(true);
    builder.build();
  }
}
