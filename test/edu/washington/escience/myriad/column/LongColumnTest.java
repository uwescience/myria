package edu.washington.escience.myriad.column;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import edu.washington.escience.myriad.proto.DataProto.ColumnMessage;

public class LongColumnTest {

  @Test
  public void testProto() {
    StringColumn original = new StringColumn();
    original.put("First").put("Second").put("Third").put("NextIsEmptyString").put("").put(
        "VeryVeryVeryVeryVeryVeryVeryVeryLongLast");
    ColumnMessage serialized = original.serializeToProto();
    StringColumn deserialized = new StringColumn(serialized);
    assertTrue(original.toString().equals(deserialized.toString()));
  }

}
