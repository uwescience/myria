package edu.washington.escience.myriad.column;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;

public class StringColumnTest {

  @Test
  public void testProto() {
    final LongColumn original = new LongColumn();
    original.put(1).put(2).put(5).put(11);
    final ColumnMessage serialized = original.serializeToProto();
    final LongColumn deserialized = new LongColumn(serialized);
    assertTrue(original.toString().equals(deserialized.toString()));
  }

}
