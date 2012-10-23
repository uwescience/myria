package edu.washington.escience.myriad.operator;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.table._TupleBatch;

public class FileScanTest {

  @Test
  public void testSimpleTwoColumnInt() throws DbException {
    String filename = "simple_two_col_int.txt";
    Schema schema = new Schema(new Type[] { Type.INT_TYPE, Type.INT_TYPE });
    assertTrue(getRowCount(filename, schema) == 7);
  }

  @Test(expected = DbException.class)
  public void testBadTwoColumnInt() throws DbException {
    String filename = "bad_two_col_int.txt";
    Schema schema = new Schema(new Type[] { Type.INT_TYPE, Type.INT_TYPE });
    assertTrue(getRowCount(filename, schema) == 7);
  }

  @Test(expected = DbException.class)
  public void testBadTwoColumnInt2() throws DbException {
    String filename = "bad_two_col_int_2.txt";
    Schema schema = new Schema(new Type[] { Type.INT_TYPE, Type.INT_TYPE });
    assertTrue(getRowCount(filename, schema) == 7);
  }

  /**
   * Helper function used to run tests.
   * 
   * @param filename the file in which the relation is stored.
   * @param schema the schema of the relation in the file.
   * @return the number of rows in the file.
   * @throws DbException if the file does not match the given Schema.
   */
  private static int getRowCount(final String filename, final Schema schema) throws DbException {
    String realFilename = "testdata" + File.separatorChar + "filescan" + File.separatorChar + filename;
    FileScan fileScan = new FileScan(realFilename, schema);

    fileScan.open();
    int count = 0;
    while (fileScan.hasNext()) {
      _TupleBatch tb = fileScan.next();
      count += tb.numOutputTuples();
    }

    return count;
  }

  @Test(expected = DbException.class)
  public void testBadTwoColumnInt3() throws DbException {
    String filename = "bad_two_col_int_3.txt";
    Schema schema = new Schema(new Type[] { Type.INT_TYPE, Type.INT_TYPE });
    assertTrue(getRowCount(filename, schema) == 7);
  }

}
