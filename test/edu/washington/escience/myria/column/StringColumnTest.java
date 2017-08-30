package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;

import org.junit.Test;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.builder.StringColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.storage.TupleUtils;

public class StringColumnTest {
  final int size = TupleUtils.getBatchSize(Type.STRING_TYPE);

  @Test
  public void testProto() {
    final StringColumnBuilder original = new StringColumnBuilder(size);
    original
        .appendString("First")
        .appendString("Second")
        .appendString("Third")
        .appendString("NextIsEmptyString")
        .appendString("")
        .appendString("VeryVeryVeryVeryVeryVeryVeryVeryLongLast");
    final ColumnMessage serialized = original.build().serializeToProto();
    final StringColumn deserialized =
        StringColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertTrue(original.build().toString().equals(deserialized.toString()));
  }

  @Test
  public void testFull() {
    final StringColumnBuilder builder = new StringColumnBuilder(size);
    for (int i = 0; i < size; i++) {
      builder.appendString("true");
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final StringColumnBuilder builder = new StringColumnBuilder(size);
    for (int i = 0; i < size; i++) {
      builder.appendString("false");
    }
    builder.appendString("true");
    builder.build();
  }
}
