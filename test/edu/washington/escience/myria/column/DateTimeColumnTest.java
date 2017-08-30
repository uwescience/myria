package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;

import org.joda.time.DateTime;
import org.junit.Test;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.builder.DateTimeColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.DateTimeUtils;

public class DateTimeColumnTest {
  final int size = TupleUtils.getBatchSize(Type.DATETIME_TYPE);

  @Test
  public void testProto() {
    final DateTimeColumnBuilder original = new DateTimeColumnBuilder(4);
    original
        .appendDateTime(DateTimeUtils.parse("2000-02-03"))
        .appendDateTime(DateTimeUtils.parse("2000-02-03"))
        .appendDateTime(DateTimeUtils.parse("2000-02-03 12:35:24"))
        .appendDateTime(DateTimeUtils.parse("2010-03-04 05:06:07"));
    final ColumnMessage serialized = original.build().serializeToProto();
    final DateTimeColumn deserialized =
        DateTimeColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertTrue(original.build().toString().equals(deserialized.toString()));
  }

  @Test
  public void testFull() {
    final DateTimeColumnBuilder builder = new DateTimeColumnBuilder(size);
    for (int i = 0; i < size; i++) {
      builder.appendDateTime(new DateTime());
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final DateTimeColumnBuilder builder = new DateTimeColumnBuilder(size);
    for (int i = 0; i < size; i++) {
      builder.appendDateTime(new DateTime());
    }
    builder.appendDateTime(new DateTime());
    builder.build();
  }
}
