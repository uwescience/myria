package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.util.Constants;

public class IntColumnTest {
  @BeforeClass
  public static void initializeBatchSize() {
    Constants.setBatchSize(100);
  }

  @Test
  public void testProto() {
    final IntColumnBuilder original = new IntColumnBuilder();
    original.append(1).append(2).append(5).append(11);
    final ColumnMessage serialized = original.build().serializeToProto();
    assertTrue(serialized.getType() == ColumnMessage.Type.INT);
    final IntColumn deserialized = IntColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertTrue(original.build().toString().equals(deserialized.toString()));
  }

  @Test
  public void testIntProtoColumn() {
    final IntColumnBuilder original = new IntColumnBuilder();
    original.append(1).append(2).append(5).append(11).append(17);
    final ColumnMessage serialized = original.build().serializeToProto();
    assertTrue(serialized.getType() == ColumnMessage.Type.INT);
    final IntProtoColumn deserialized = new IntProtoColumn(serialized.getIntColumn());
    assertTrue(original.build().toString().equals(deserialized.toString()));
  }

  @Test
  public void testFull() {
    final IntColumnBuilder builder = new IntColumnBuilder();
    for (int i = 0; i < Constants.getBatchSize(); i++) {
      builder.append(i);
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final IntColumnBuilder builder = new IntColumnBuilder();
    for (int i = 0; i < Constants.getBatchSize(); i++) {
      builder.append(i);
    }
    builder.append(Constants.getBatchSize());
    builder.build();
  }

}
