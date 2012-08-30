package edu.washington.escience.myriad.column;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage;

public class DoubleColumnTest {

  @Test
  public void testProto() {
    DoubleColumn original = new DoubleColumn();
    original.put(1).put(2).put(5).put(11);
    ColumnMessage serialized = original.serializeToProto();
    DoubleColumn deserialized = new DoubleColumn(serialized);
    assertTrue(original.toString().equals(deserialized.toString()));
  }

}
