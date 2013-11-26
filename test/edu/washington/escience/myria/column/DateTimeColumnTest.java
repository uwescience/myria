package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;

import org.joda.time.DateTime;
import org.junit.Test;

import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.column.DateTimeColumn;
import edu.washington.escience.myria.column.builder.DateTimeColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.util.DateTimeUtils;

public class DateTimeColumnTest {

  @Test
  public void testProto() {
    final DateTimeColumnBuilder original = new DateTimeColumnBuilder();
    original.append(DateTimeUtils.parse("2000-02-03")).append(DateTimeUtils.parse("2000-02-03")).append(
        DateTimeUtils.parse("2000-02-03 12:35:24")).append(DateTimeUtils.parse("2010-03-04 05:06:07"));
    final ColumnMessage serialized = original.build().serializeToProto();
    final DateTimeColumn deserialized = DateTimeColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertTrue(original.build().toString().equals(deserialized.toString()));
  }

  @Test
  public void testFull() {
    final DateTimeColumnBuilder builder = new DateTimeColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.append(new DateTime());
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final DateTimeColumnBuilder builder = new DateTimeColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.append(new DateTime());
    }
    builder.append(new DateTime());
    builder.build();
  }

}
