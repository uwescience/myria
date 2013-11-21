package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.util.Constants;

public class DoubleColumnTest {
  @BeforeClass
  public static void initializeBatchSize() {
    Constants.setBatchSize(100);
  }

  @Test
  public void testProto() {
    final DoubleColumnBuilder original = new DoubleColumnBuilder();
    original.append(1).append(2).append(5).append(11);
    final ColumnMessage serialized = original.build().serializeToProto();
    final DoubleColumn deserialized = DoubleColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertTrue(original.build().toString().equals(deserialized.toString()));
  }

  @Test
  public void testFull() {
    final DoubleColumnBuilder builder = new DoubleColumnBuilder();
    for (int i = 0; i < Constants.getBatchSize(); i++) {
      builder.append(i * 1.0);
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final DoubleColumnBuilder builder = new DoubleColumnBuilder();
    for (int i = 0; i < Constants.getBatchSize(); i++) {
      builder.append(i * 1.0);
    }
    builder.append(0.0);
    builder.build();
  }

}
