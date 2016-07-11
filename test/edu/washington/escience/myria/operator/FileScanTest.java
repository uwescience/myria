package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.nio.file.Paths;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.io.ByteArraySource;
import edu.washington.escience.myria.storage.TupleBatch;

public class FileScanTest {

  /**
   * Helper function used to run tests.
   *
   * @param filename the file in which the relation is stored.
   * @param schema the schema of the relation in the file.
   * @return the number of rows in the file.
   * @throws DbException if the file does not match the given Schema.
   * @throws InterruptedException
   */
  private static int getRowCount(final String filename, final Schema schema)
      throws DbException, InterruptedException {
    return getRowCount(filename, schema, null);
  }

  /**
   * Helper function used to run tests.
   *
   * @param filename the file in which the relation is stored.
   * @param schema the schema of the relation in the file.
   * @param delimiter if non-null, an override file delimiter
   * @return the number of rows in the file.
   * @throws DbException if the file does not match the given Schema.
   * @throws FileNotFoundException if the specified file does not exist.
   * @throws InterruptedException
   */
  private static int getRowCount(
      final String filename, final Schema schema, final Character delimiter)
      throws DbException, InterruptedException {
    return getRowCount(filename, schema, delimiter, null, null);
  }

  /**
   * @param filename the file in which the relation is stored.
   * @param schema the schema of the relation in the file.
   * @param delimiter if non-null, an override file delimiter
   * @param quote the user specified quotation mark
   * @return the number of rows in the file.
   * @throws DbException if the file does not match the given Schema.
   * @throws FileNotFoundException if the specified file does not exist.
   * @throws InterruptedException
   */
  private static int getRowCount(
      final String filename, final Schema schema, final Character delimiter, final Character quote)
      throws DbException, InterruptedException {
    return getRowCount(filename, schema, delimiter, quote, null);
  }

  /**
   * @param filename the file in which the relation is stored.
   * @param schema the schema of the relation in the file.
   * @param delimiter if non-null, an override file delimiter
   * @param quote the user specified quotation mark
   * @return the number of rows in the file.
   * @throws DbException if the file does not match the given Schema.
   * @throws FileNotFoundException if the specified file does not exist.
   * @throws InterruptedException
   */
  private static int getRowCount(
      final String filename,
      final Schema schema,
      final Character delimiter,
      final Character quote,
      final Character escape)
      throws DbException, InterruptedException {
    final String realFilename = Paths.get("testdata", "filescan", filename).toString();
    FileScan fileScan = new FileScan(realFilename, schema, delimiter, quote, escape, null);
    return getRowCount(fileScan);
  }

  /**
   * Helper function used to run tests.
   *
   * @param fileScan the FileScan object to be tested.
   * @return the number of rows in the file.
   * @throws DbException if the file does not match the given Schema.
   * @throws InterruptedException
   */
  private static int getRowCount(final FileScan fileScan) throws DbException, InterruptedException {
    fileScan.open(null);

    int count = 0;
    TupleBatch tb = null;
    while (!fileScan.eos()) {
      tb = fileScan.nextReady();
      if (tb != null) {
        count += tb.numTuples();
      }
    }

    return count;
  }

  @Test
  public void testSimpleCsvEscape() throws DbException, InterruptedException {
    final String filename = "two_col_string.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.STRING_TYPE, Type.STRING_TYPE));
    assertEquals(7, getRowCount(filename, schema));
  }

  @Test
  public void testSimpleCsvQuoted() throws DbException, InterruptedException {
    final String filename = "two_col_string_quoted.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.STRING_TYPE, Type.STRING_TYPE));
    assertEquals(7, getRowCount(filename, schema));
  }

  @Test
  public void testSimpleCsvSingleQuoted() throws DbException, InterruptedException {
    final String filename = "two_col_string_single_quoted.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.STRING_TYPE, Type.STRING_TYPE));
    assertEquals(7, getRowCount(filename, schema, null, '\''));
  }

  @Test
  public void testSimpleCsvEscaped() throws DbException, InterruptedException {
    final String filename = "two_col_string_escaped.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.STRING_TYPE, Type.STRING_TYPE));
    assertEquals(7, getRowCount(filename, schema, null, null, '\\'));
  }

  @Test(expected = DbException.class)
  public void testBadCommaTwoColumnInt() throws DbException, InterruptedException {
    final String filename = "bad_comma_two_col_int_unix.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    assertEquals(7, getRowCount(filename, schema));
  }

  @Test(expected = DbException.class)
  public void testBadTwoColumnInt() throws DbException, InterruptedException {
    final String filename = "bad_two_col_int.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    assertEquals(7, getRowCount(filename, schema));
  }

  @Test(expected = DbException.class)
  public void testBadTwoColumnInt2() throws DbException, InterruptedException {
    final String filename = "bad_two_col_int_2.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    assertEquals(7, getRowCount(filename, schema));
  }

  @Test(expected = DbException.class)
  public void testBadTwoColumnInt3() throws DbException, InterruptedException {
    final String filename = "bad_two_col_int_3.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    assertEquals(7, getRowCount(filename, schema));
  }

  @Test
  public void testCommaTwoColumnIntUnix() throws DbException, InterruptedException {
    final String filename = "comma_two_col_int_unix.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    assertEquals(7, getRowCount(filename, schema, ','));
  }

  @Test
  public void testCommaTwoColumnIntUnixNoTrailingNewline()
      throws DbException, InterruptedException {
    final String filename = "comma_two_col_int_unix_no_trailing_newline.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    assertEquals(7, getRowCount(filename, schema, ','));
  }

  @Test
  public void testCommaTwoColumnIntDos() throws DbException, InterruptedException {
    final String filename = "comma_two_col_int_dos.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    int countRow = getRowCount(filename, schema, ',');
    assertEquals(7, countRow);
  }

  @Test
  public void testSimpleTwoColumnInt() throws DbException, InterruptedException {
    final String filename = "simple_two_col_int.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    assertEquals(7, getRowCount(filename, schema, ' '));
  }

  @Test
  public void testSimpleTwoColumnFloat() throws Exception {
    final String filename = "simple_two_col_float.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.FLOAT_TYPE, Type.FLOAT_TYPE));
    assertEquals(7, getRowCount(filename, schema, ' '));
  }

  @Test
  public void testRandomCSV() throws Exception {
    // file generated using:
    // python testdata/generated/generate_csv.py 10000 --delimiter ' ' int int float str > testdata/filescan/random.csv
    final String filename = "random.csv";
    final Schema schema =
        new Schema(
            ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE, Type.FLOAT_TYPE, Type.STRING_TYPE));
    assertEquals(10000, getRowCount(filename, schema, ' '));
  }

  @Test
  public void testBigFile() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    PrintStream printedBytes = new PrintStream(bytes);
    /* Print 2*TupleBatch.BATCH_SIZE lines */
    for (int i = 0; i < TupleBatch.BATCH_SIZE * 2; ++i) {
      printedBytes.print(i);
      printedBytes.print('\n');
    }
    printedBytes.flush();
    FileScan scanBytes =
        new FileScan(
            new ByteArraySource(bytes.toByteArray()),
            Schema.of(ImmutableList.of(Type.INT_TYPE), ImmutableList.of("col1")));
    assertEquals(2 * TupleBatch.BATCH_SIZE, getRowCount(scanBytes));
  }

  @Test
  public void testPipeDelimiter() throws Exception {
    final String filename = "nccdc_100.txt";
    final Schema schema =
        new Schema(
            ImmutableList.of(
                Type.STRING_TYPE,
                Type.STRING_TYPE,
                Type.INT_TYPE,
                Type.INT_TYPE,
                Type.INT_TYPE,
                Type.INT_TYPE,
                Type.INT_TYPE));
    assertEquals(100, getRowCount(filename, schema, '|'));
  }
}
