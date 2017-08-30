package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

import edu.washington.escience.myria.column.builder.BlobColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;

public class BlobColumnTest {

  @Test
  public void testProto() {
    final BlobColumnBuilder original = new BlobColumnBuilder(1);
    original.appendBlob(ByteBuffer.wrap("Test1".getBytes()));
    final ColumnMessage serialized = original.build().serializeToProto();
    final BlobColumn deserialized =
        BlobColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertTrue(original.size() == deserialized.size());
  }

  @Test(expected = BufferOverflowException.class)
  public void testFull() {
    final BlobColumnBuilder original = new BlobColumnBuilder(6);
    original
        .appendBlob(ByteBuffer.wrap("First".getBytes()))
        .appendBlob(ByteBuffer.wrap("Second".getBytes()))
        .appendBlob(ByteBuffer.wrap("Third".getBytes()))
        .appendBlob(ByteBuffer.wrap("NextIsEmptyString".getBytes()))
        .appendBlob(ByteBuffer.wrap("".getBytes()))
        .appendBlob(ByteBuffer.wrap("VeryVeryVeryVeryVeryVeryVeryVeryLongLast".getBytes()));
    original.build();
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
