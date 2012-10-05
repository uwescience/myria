package edu.washington.escience.myriad.column;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;

public class BooleanColumnTest {

  @Test
  public void testProto() {
    final BooleanColumn original = new BooleanColumn();
    original.put(true).put(false).put(true).put(false).put(false).put(false).put(false).put(false).put(true).put(false)
        .put(false).put(false).put(false).put(false);
    final ColumnMessage serialized = original.serializeToProto();
    final BooleanColumn deserialized = new BooleanColumn(serialized);
    assertTrue(original.toString().equals(deserialized.toString()));
  }

}
