package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;

import org.junit.Test;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.column.StringColumn;
import edu.washington.escience.myria.column.builder.StringColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;

public class StringColumnTest {

  @Test
  public void testProto() {
    final StringColumnBuilder original = new StringColumnBuilder();
    original.append("First").append("Second").append("Third").append("NextIsEmptyString").append("").append(
        "VeryVeryVeryVeryVeryVeryVeryVeryLongLast");
    final ColumnMessage serialized = original.build().serializeToProto();
    final StringColumn deserialized = StringColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertTrue(original.build().toString().equals(deserialized.toString()));
  }

  @Test
  public void testFull() {
    final StringColumnBuilder builder = new StringColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.append("true");
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final StringColumnBuilder builder = new StringColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.append("false");
    }
    builder.append("true");
    builder.build();
  }

}
