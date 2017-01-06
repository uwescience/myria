package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import edu.washington.escience.myria.column.BlobColumn;
import edu.washington.escience.myria.column.builder.BlobColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;

public class BlobColumnTest {

  @Test
  public void testProto() {

    final BlobColumnBuilder original = new BlobColumnBuilder();

    original.appendBlob(ByteBuffer.wrap("Test1".getBytes()));

    final ColumnMessage serialized = original.build().serializeToProto();
    final BlobColumn deserialized =
        BlobColumnBuilder.buildFromProtobuf(serialized, original.size());

    assertTrue(original.size() == deserialized.size());
  }

  @Test
  public void testFull() {

    final BlobColumnBuilder builder = new BlobColumnBuilder();
    for (int i = 0; i < 1; i++) {
      builder.appendBlob(ByteBuffer.wrap(readbb()));
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
