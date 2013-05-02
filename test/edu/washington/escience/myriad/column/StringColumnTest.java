package edu.washington.escience.myriad.column;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;

public class StringColumnTest {

  @Test
  public void testProto() {
    final StringColumn original = new StringColumn();
    original.put("First").put("Second").put("Third").put("NextIsEmptyString").put("").put(
        "VeryVeryVeryVeryVeryVeryVeryVeryLongLast");
    final ColumnMessage serialized = original.serializeToProto();
    final StringColumn deserialized = new StringColumn(serialized, original.size());
    assertTrue(original.toString().equals(deserialized.toString()));
  }

}
