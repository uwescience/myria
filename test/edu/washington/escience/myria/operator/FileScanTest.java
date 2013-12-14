package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.Type;

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
  private static int getRowCount(final String filename, final Schema schema) throws DbException, InterruptedException {
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
  private static int getRowCount(final String filename, final Schema schema, final String delimiter)
      throws DbException, InterruptedException {
    final String realFilename = "testdata" + File.separatorChar + "filescan" + File.separatorChar + filename;
    FileScan fileScan;
    try {
      fileScan = new FileScan(realFilename, schema, delimiter);
    } catch (FileNotFoundException e) {
      throw new DbException(e);
    }
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
  private static int getRowCount(FileScan fileScan) throws DbException, InterruptedException {
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

  @Test(expected = DbException.class)
  public void testBadCommaTwoColumnInt() throws DbException, InterruptedException {
    final String filename = "comma_two_col_int_unix.txt";
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
    assertEquals(7, getRowCount(filename, schema, ","));
  }

  @Test
  public void testCommaTwoColumnIntUnixNoTrailingNewline() throws DbException, InterruptedException {
    final String filename = "comma_two_col_int_unix_no_trailing_newline.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    assertEquals(7, getRowCount(filename, schema, ","));
  }

  @Test
  public void testCommaTwoColumnIntDos() throws DbException, InterruptedException {
    final String filename = "comma_two_col_int_dos.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    assertEquals(7, getRowCount(filename, schema, ","));
  }

  @Test
  public void testSimpleTwoColumnInt() throws DbException, InterruptedException {
    final String filename = "simple_two_col_int.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    assertEquals(7, getRowCount(filename, schema));
  }

  @Test
  public void testSimpleTwoColumnFloat() throws Exception {
    final String filename = "simple_two_col_float.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.FLOAT_TYPE, Type.FLOAT_TYPE));
    assertEquals(7, getRowCount(filename, schema));
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
    FileScan scanBytes = new FileScan(Schema.of(ImmutableList.of(Type.INT_TYPE), ImmutableList.of("col1")));
    scanBytes.setInputStream(new ByteArrayInputStream(bytes.toByteArray()));
    assertEquals(2 * TupleBatch.BATCH_SIZE, getRowCount(scanBytes));
  }

  @Test
  public void testPipeDelimiter() throws Exception {
    final String filename = "nccdc_100.txt";
    final Schema schema =
        new Schema(ImmutableList.of(Type.STRING_TYPE, Type.STRING_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE,
            Type.INT_TYPE, Type.INT_TYPE));
    assertEquals(100, getRowCount(filename, schema, "|"));
  }
}
