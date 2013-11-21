package edu.washington.escience.myria.column;

import static org.junit.Assert.assertEquals;

import java.nio.BufferOverflowException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.util.Constants;

public class FloatColumnTest {
  @BeforeClass
  public static void initializeBatchSize() {
    Constants.setBatchSize(100);
  }

  @Test
  public void testProto() {
    final FloatColumnBuilder original = new FloatColumnBuilder();
    original.append(1.0f).append(2.0f).append(5.0f).append(11.0f);
    FloatColumn column = original.build();
    final ColumnMessage serialized = column.serializeToProto();
    final FloatColumn deserialized = FloatColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertEquals(column.get(0), deserialized.get(0));
    assertEquals(column.get(1), deserialized.get(1));
    assertEquals(column.get(2), deserialized.get(2));
    assertEquals(column.get(3), deserialized.get(3));
  }

  @Test
  public void testFull() {
    final FloatColumnBuilder builder = new FloatColumnBuilder();
    for (int i = 0; i < Constants.getBatchSize(); i++) {
      builder.append(i * 1.0f);
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final FloatColumnBuilder builder = new FloatColumnBuilder();
    for (int i = 0; i < Constants.getBatchSize(); i++) {
      builder.append(i * 1.0f);
    }
    builder.append(0.0f);
    builder.build();
  }

}
