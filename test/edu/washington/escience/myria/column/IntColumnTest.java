package edu.washington.escience.myria.column;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;

import org.junit.Test;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.builder.IntColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.storage.TupleUtils;

public class IntColumnTest {
  final int size = TupleUtils.getBatchSize(Type.INT_TYPE);

  @Test
  public void testProto() {
    final IntColumnBuilder original = new IntColumnBuilder(size);
    original.appendInt(1).appendInt(2).appendInt(5).appendInt(11);
    final ColumnMessage serialized = original.build().serializeToProto();
    assertEquals(ColumnMessage.Type.INT, serialized.getType());
    final IntColumn deserialized = IntColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertEquals(original.build().toString(), deserialized.toString());
  }

  @Test
  public void testIntProtoColumn() {
    final IntColumnBuilder original = new IntColumnBuilder(size);
    original.appendInt(1).appendInt(2).appendInt(5).appendInt(11).appendInt(17);
    final ColumnMessage serialized = original.build().serializeToProto();
    assertEquals(ColumnMessage.Type.INT, serialized.getType());
    final IntProtoColumn deserialized = new IntProtoColumn(serialized.getIntColumn());
    assertTrue(original.build().toString().equals(deserialized.toString()));
  }

  @Test
  public void testFull() {
    final IntColumnBuilder builder = new IntColumnBuilder(size);
    for (int i = 0; i < size; i++) {
      builder.appendInt(i);
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final IntColumnBuilder builder = new IntColumnBuilder(size);
    for (int i = 0; i < size; i++) {
      builder.appendInt(i);
    }
    builder.appendInt(0);
    builder.build();
  }
}
