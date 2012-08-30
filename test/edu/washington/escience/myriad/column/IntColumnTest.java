package edu.washington.escience.myriad.column;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage;

public class IntColumnTest {

  @Test
  public void testProto() {
    IntColumn original = new IntColumn();
    original.put(1).put(2).put(5).put(11);
    ColumnMessage serialized = original.serializeToProto();
    IntColumn deserialized = new IntColumn(serialized);
    assertTrue(original.toString().equals(deserialized.toString()));
  }

}
