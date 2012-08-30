package edu.washington.escience.myriad.column;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage;

public class BooleanColumnTest {

  @Test
  public void testProto() {
    BooleanColumn original = new BooleanColumn();
    original.put(true).put(false).put(true).put(false).put(false).put(false).put(false).put(false).put(true).put(false)
        .put(false).put(false).put(false).put(false);
    ColumnMessage serialized = original.serializeToProto();
    BooleanColumn deserialized = new BooleanColumn(serialized);
    assertTrue(original.toString().equals(deserialized.toString()));
  }

}
