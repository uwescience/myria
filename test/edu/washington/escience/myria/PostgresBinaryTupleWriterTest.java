package edu.washington.escience.myria;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.storage.TupleBuffer;

public class PostgresBinaryTupleWriterTest {

  @Test
  public void testBinaryOutput() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    PostgresBinaryTupleWriter writer = new PostgresBinaryTupleWriter();
    writer.open(out);

    TupleBuffer tuples =
        new TupleBuffer(
            new Schema(
                ImmutableList.of(
                    Type.BOOLEAN_TYPE,
                    Type.INT_TYPE,
                    Type.LONG_TYPE,
                    Type.FLOAT_TYPE,
                    Type.DOUBLE_TYPE,
                    Type.STRING_TYPE,
                    Type.DATETIME_TYPE)));

    tuples.putBoolean(0, true);
    tuples.putInt(1, 1);
    tuples.putLong(2, 100L);
    tuples.putFloat(3, 3.14f);
    tuples.putDouble(4, 3.14);
    tuples.putString(5, "one");
    tuples.putDateTime(6, new DateTime(1990, 7, 18, 2, 3, 10));

    tuples.putBoolean(0, false);
    tuples.putInt(1, 2);
    tuples.putLong(2, 200L);
    tuples.putFloat(3, 3.14f);
    tuples.putDouble(4, -3.14);
    tuples.putString(5, "two");
    tuples.putDateTime(6, new DateTime(2013, 9, 30, 3, 1, 10));

    tuples.putBoolean(0, true);
    tuples.putInt(1, 3);
    tuples.putLong(2, 300L);
    tuples.putFloat(3, 3.14f);
    tuples.putDouble(4, 123.456);
    tuples.putString(5, "three");
    tuples.putDateTime(6, new DateTime(2000, 1, 1, 0, 0, 0));

    writer.writeTuples(tuples);
    writer.done();

    byte[] actual = out.toByteArray();

    /*
     * // generate file:
     *
     * create table foo(a bool, b int, c bigint, d real, e double precision, f text, g timestamp);
     *
     * insert into foo values (true, 1, 100, 3.14, 3.14, 'one', '1990-07-18 02:03:10'), (false, 2, 200, 3.14, -3.14,
     * 'two', '2013-09-30 03:01:10'), (true, 3, 300, 3.14, 123.456, 'three', '2000-01-01 00:00:00');
     *
     * copy foo to '/private/tmp/pg.bin' with binary;
     */

    Path filename = Paths.get(Paths.get("testdata", "tuplewriter", "pg.bin").toString());
    byte[] expected = Files.readAllBytes(filename);

    assertEquals(expected.length, actual.length);
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], actual[i]);
    }
  }
}
