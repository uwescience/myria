package edu.washington.escience.myriad.column;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;

public class FloatColumnTest {

  @Test
  public void testProto() {
    final FloatColumn original = new FloatColumn();
    original.put(1).put(2).put(5).put(11);
    final ColumnMessage serialized = original.serializeToProto();
    final FloatColumn deserialized = new FloatColumn(serialized, original.size());
    assertTrue(original.toString().equals(deserialized.toString()));
  }

}
