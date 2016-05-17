package edu.washington.escience.myria;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.storage.TupleBuffer;

public class CSVTupleWriterTest {

  @Test
  public void testConstructionWithNoNames() throws IOException {
    OutputStream out = new ByteArrayOutputStream();

    CsvTupleWriter writer = new CsvTupleWriter();
    writer.open(out);
    writer.writeColumnHeaders(ImmutableList.of("foo", "bar", "baz"));

    TupleBuffer tuples =
        new TupleBuffer(
            new Schema(ImmutableList.of(Type.STRING_TYPE, Type.STRING_TYPE, Type.STRING_TYPE)));
    tuples.putString(0, "0");
    tuples.putString(1, "hello world");
    tuples.putString(2, "a, b");
    tuples.putString(0, "1");
    tuples.putString(1, "\"heya\"");
    tuples.putString(2, "last");

    writer.writeTuples(tuples);
    writer.done();

    assertEquals(
        "foo,bar,baz\r\n0,hello world,\"a, b\"\r\n1,\"\"\"heya\"\"\",last\r\n", out.toString());
  }

  @Test
  public void testDelimiter() throws IOException {
    OutputStream out = new ByteArrayOutputStream();
    // use an uncommon one for testing
    CsvTupleWriter writer = new CsvTupleWriter('_');
    writer.open(out);
    writer.writeColumnHeaders(ImmutableList.of("foo", "bar"));
    TupleBuffer tuples =
        new TupleBuffer(new Schema(ImmutableList.of(Type.STRING_TYPE, Type.INT_TYPE)));
    tuples.putString(0, "a");
    tuples.putInt(1, 1);
    tuples.putString(0, "b");
    tuples.putInt(1, 2);
    writer.writeTuples(tuples);
    writer.done();
    assertEquals("foo_bar\r\na_1\r\nb_2\r\n", out.toString());
  }
}
