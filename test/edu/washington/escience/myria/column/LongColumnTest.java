package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.util.Constants;

public class LongColumnTest {
  @BeforeClass
  public static void initializeBatchSize() {
    Constants.setBatchSize(100);
  }

  @Test
  public void testProto() {
    final LongColumnBuilder original = new LongColumnBuilder();
    original.append(1).append(2).append(5).append(11);
    final ColumnMessage serialized = original.build().serializeToProto();
    final LongColumn deserialized = LongColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertTrue(original.build().toString().equals(deserialized.toString()));
  }

  @Test
  public void testFull() {
    final LongColumnBuilder builder = new LongColumnBuilder();
    for (int i = 0; i < Constants.getBatchSize(); i++) {
      builder.append(i);
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final LongColumnBuilder builder = new LongColumnBuilder();
    for (int i = 0; i < Constants.getBatchSize(); i++) {
      builder.append(i);
    }
    builder.append(0);
    builder.build();
  }

}
