package edu.washington.escience.myria.column;

import static org.junit.Assert.assertEquals;

import java.nio.BufferOverflowException;

import org.junit.Test;

import edu.washington.escience.myria.column.FloatColumn;
import edu.washington.escience.myria.column.builder.FloatColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.storage.TupleBatch;

public class FloatColumnTest {

  @Test
  public void testProto() {
    final FloatColumnBuilder original = new FloatColumnBuilder();
    original.appendFloat(1.0f).appendFloat(2.0f).appendFloat(5.0f).appendFloat(11.0f);
    FloatColumn column = original.build();
    final ColumnMessage serialized = column.serializeToProto();
    final FloatColumn deserialized =
        FloatColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertEquals(column.getObject(0), deserialized.getObject(0));
    assertEquals(column.getObject(1), deserialized.getObject(1));
    assertEquals(column.getObject(2), deserialized.getObject(2));
    assertEquals(column.getObject(3), deserialized.getObject(3));
  }

  @Test
  public void testFull() {
    final FloatColumnBuilder builder = new FloatColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.appendFloat(i * 1.0f);
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final FloatColumnBuilder builder = new FloatColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.appendFloat(i * 1.0f);
    }
    builder.appendFloat(0.0f);
    builder.build();
  }
}
