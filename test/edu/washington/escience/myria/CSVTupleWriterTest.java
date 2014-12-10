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

    CsvTupleWriter writer = new CsvTupleWriter(out);
    writer.writeColumnHeaders(ImmutableList.of("foo", "bar", "baz"));

    TupleBuffer tuples =
        new TupleBuffer(new Schema(ImmutableList.of(Type.STRING_TYPE, Type.STRING_TYPE, Type.STRING_TYPE)));
    tuples.putString(0, "0");
    tuples.putString(1, "hello world");
    tuples.putString(2, "a, b");
    tuples.putString(0, "1");
    tuples.putString(1, "\"heya\"");
    tuples.putString(2, "last");

    writer.writeTuples(tuples);
    writer.done();

    assertEquals("foo,bar,baz\n0,hello world,\"a, b\"\n1,\"\"\"heya\"\"\",last\n", out.toString());
  }
}
