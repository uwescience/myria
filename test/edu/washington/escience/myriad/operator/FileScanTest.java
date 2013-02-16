package edu.washington.escience.myriad.operator;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;

public class FileScanTest {

  /**
   * Helper function used to run tests.
   * 
   * @param filename the file in which the relation is stored.
   * @param schema the schema of the relation in the file.
   * @return the number of rows in the file.
   * @throws DbException if the file does not match the given Schema.
   */
  private static int getRowCount(final String filename, final Schema schema) throws DbException {
    return getRowCount(filename, schema, false);
  }

  /**
   * Helper function used to run tests.
   * 
   * @param filename the file in which the relation is stored.
   * @param schema the schema of the relation in the file.
   * @param commaIsDelimiter true if commas should be considered delimiting characters.
   * @return the number of rows in the file.
   * @throws DbException if the file does not match the given Schema.
   * @throws FileNotFoundException if the specified file does not exist.
   */
  private static int getRowCount(final String filename, final Schema schema, final boolean commaIsDelimiter)
      throws DbException {
    final String realFilename = "testdata" + File.separatorChar + "filescan" + File.separatorChar + filename;
    FileScan fileScan;
    try {
      fileScan = new FileScan(realFilename, schema, commaIsDelimiter);
    } catch (FileNotFoundException e) {
      throw new DbException(e);
    }

    fileScan.open();
    int count = 0;
    TupleBatch tb = null;
    while ((tb = fileScan.next()) != null) {
      count += tb.numTuples();
    }

    return count;
  }

  @Test(expected = DbException.class)
  public void testBadCommaTwoColumnInt() throws DbException {
    final String filename = "comma_two_col_int.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    assertTrue(getRowCount(filename, schema) == 7);
  }

  @Test(expected = DbException.class)
  public void testBadTwoColumnInt() throws DbException {
    final String filename = "bad_two_col_int.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    assertTrue(getRowCount(filename, schema) == 7);
  }

  @Test(expected = DbException.class)
  public void testBadTwoColumnInt2() throws DbException {
    final String filename = "bad_two_col_int_2.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    assertTrue(getRowCount(filename, schema) == 7);
  }

  @Test(expected = DbException.class)
  public void testBadTwoColumnInt3() throws DbException {
    final String filename = "bad_two_col_int_3.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    assertTrue(getRowCount(filename, schema) == 7);
  }

  @Test
  public void testCommaTwoColumnInt() throws DbException {
    final String filename = "comma_two_col_int.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    assertTrue(getRowCount(filename, schema, true) == 7);
  }

  @Test
  public void testSimpleTwoColumnInt() throws DbException {
    final String filename = "simple_two_col_int.txt";
    final Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    assertTrue(getRowCount(filename, schema) == 7);
  }

}
