package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import edu.washington.escience.myria.column.BytesColumn;
import edu.washington.escience.myria.column.builder.BytesColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;

public class BytesColumnTest {

  @Test
  public void testProto() {

    final BytesColumnBuilder original = new BytesColumnBuilder();

    original.appendByteBuffer(ByteBuffer.wrap("Test1".getBytes()));

    final ColumnMessage serialized = original.build().serializeToProto();
    final BytesColumn deserialized =
        BytesColumnBuilder.buildFromProtobuf(serialized, original.size());
    //System.out.print(original.build().toString());
    //System.out.print(deserialized.toString());

    assertTrue(original.size() == deserialized.size());
  }

  @Test
  public void testFull() {

    final BytesColumnBuilder builder = new BytesColumnBuilder();
    for (int i = 0; i < 1; i++) {
      builder.appendByteBuffer(ByteBuffer.wrap(readbb()));
    }
    builder.build();
  }

  protected byte[] readbb() {
    Path filename = Paths.get(Paths.get("testdata", "pythonUDF", "1.p").toString());

    byte[] data = null;
    try {
      data = Files.readAllBytes(filename);
    } catch (IOException e) {

      e.printStackTrace();
    }
    return data;
  }
}
