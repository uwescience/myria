package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.util.Constants;

public class StringColumnTest {
  @BeforeClass
  public static void initializeBatchSize() {
    Constants.setBatchSize(100);
  }

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
    for (int i = 0; i < Constants.getBatchSize(); i++) {
      builder.append("true");
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final StringColumnBuilder builder = new StringColumnBuilder();
    for (int i = 0; i < Constants.getBatchSize(); i++) {
      builder.append("false");
    }
    builder.append("true");
    builder.build();
  }

}
