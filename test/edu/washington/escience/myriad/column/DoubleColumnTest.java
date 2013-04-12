package edu.washington.escience.myriad.column;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;

public class DoubleColumnTest {

  @Test
  public void testProto() {
    final DoubleColumn original = new DoubleColumn();
    original.put(1).put(2).put(5).put(11);
    final ColumnMessage serialized = original.serializeToProto();
    final DoubleColumn deserialized = new DoubleColumn(serialized, original.size());
    assertTrue(original.toString().equals(deserialized.toString()));
  }

}
