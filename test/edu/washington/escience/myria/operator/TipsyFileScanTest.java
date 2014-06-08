package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.junit.Test;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.ConstantExpression;
import edu.washington.escience.myria.expression.EqualsExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.TestEnvVars;

/**
 * 
 * @author leelee
 * 
 */
public class TipsyFileScanTest {

  private static final int NUM_COLUMN_GAS = 12;
  private static final int NUM_COLUMN_DARK = 9;
  private static final int NUM_COLUMN_STAR = 11;

  public static int getRowCount(Operator fileScan) throws DbException {
    fileScan.open(TestEnvVars.get());

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
  public void testSimpleBigEndian() throws IOException, DbException {
    String binFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "tipsy1";
    String iOrderFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "iOrder1.iord";
    String grpFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "grp1.amiga.grp";
    int ngas = 1;
    int ndark = 1;
    int nstar = 1;
    int ntot = ngas + ndark + nstar;
    TipsyFileScan filescan = new TipsyFileScan(binFilename, iOrderFilename, grpFilename);
    assertEquals(ntot, getRowCount(filescan));

  }

  @Test
  public void testSimpleBigEndianStar() throws IOException, DbException {
    String binFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "tipsy3";
    String iOrderFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "iOrder3.iord";
    String grpFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "grp3.amiga.grp";
    TipsyFileScan filescan = new TipsyFileScan(binFilename, iOrderFilename, grpFilename);
    ExpressionOperator expr =
        new EqualsExpression(new VariableExpression(16), new ConstantExpression(Type.STRING_TYPE, "star"));
    Filter filter = new Filter(new Expression(null, expr), filescan);
    assertEquals(3, getRowCount(filter));
  }

  @Test
  public void testSimpleBigEndianDark() throws IOException, DbException {
    String binFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "tipsy3";
    String iOrderFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "iOrder3.iord";
    String grpFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "grp3.amiga.grp";
    TipsyFileScan filescan = new TipsyFileScan(binFilename, iOrderFilename, grpFilename);
    ExpressionOperator expr =
        new EqualsExpression(new VariableExpression(16), new ConstantExpression(Type.STRING_TYPE, "dark"));
    Filter filter = new Filter(new Expression(null, expr), filescan);
    assertEquals(2, getRowCount(filter));
  }

  @Test
  public void testSimpleBigEndianGas() throws IOException, DbException {
    String binFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "tipsy3";
    String iOrderFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "iOrder3.iord";
    String grpFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "grp3.amiga.grp";
    TipsyFileScan filescan = new TipsyFileScan(binFilename, iOrderFilename, grpFilename);
    ExpressionOperator expr =
        new EqualsExpression(new VariableExpression(16), new ConstantExpression(Type.STRING_TYPE, "gas"));
    Filter filter = new Filter(new Expression(null, expr), filescan);
    assertEquals(4, getRowCount(filter));
  }

  // @Test
  // the data is stored in /projects/db8/dataset_astro_2011/
  // this test took 161796 ms which is about 2.7 minutes on a i7 processor 8gb ram machine
  public void testCosmo512Star() throws DbException {
    String binFileName =
        "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "cosmo50cmb.256g2MbwK.00512";
    String iOrderFileName =
        "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "cosmo50cmb.256g2MbwK.00512.iord";
    String grpFileName =
        "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "cosmo50cmb.256g2MbwK.00512.amiga.grp";
    TipsyFileScan tfScan = new TipsyFileScan(binFileName, iOrderFileName, grpFileName);
    ExpressionOperator expr =
        new EqualsExpression(new VariableExpression(16), new ConstantExpression(Type.STRING_TYPE, "star"));
    Filter filter = new Filter(new Expression(null, expr), tfScan);
    assertEquals(6949401, getRowCount(filter));
  }

  /**
   * Generate big endian binary file.
   * 
   * @param filename The filename to generate the content.
   * @param ngas The number of gas records.
   * @param ndark The number of dark records.
   * @param nstar The number of star records.
   * @throws IOException if error writing to file.
   */
  public static void generateTipsyFile(String filename, int ngas, int ndark, int nstar) throws IOException {
    FileOutputStream fStream = new FileOutputStream(filename);
    DataOutputStream output = new DataOutputStream(fStream);
    // write header
    // time
    output.writeDouble(System.currentTimeMillis());
    // total
    output.writeInt(nstar + ngas + ndark);
    // random number to skip 4 bytes
    output.writeInt(0);
    // ngas
    output.writeInt(ngas);
    // ndark
    output.writeInt(ndark);
    // nstar
    output.writeInt(nstar);
    // random number to skip 4 bytes
    output.writeInt(0);
    // write gas records
    for (int i = 0; i < ngas; i++) {
      for (int j = 0; j < NUM_COLUMN_GAS; j++) {
        output.writeFloat(j);
      }
    }
    // write dark records
    for (int i = 0; i < ndark; i++) {
      for (int j = 0; j < NUM_COLUMN_DARK; j++) {
        output.writeFloat(j);
      }
    }
    // write star records
    for (int i = 0; i < nstar; i++) {
      for (int j = 0; j < NUM_COLUMN_STAR; j++) {
        output.writeFloat(j);
      }
    }
    output.close();
  }

  /**
   * Generate ascii file that contains row number of lines.
   * 
   * @param fileName The filename to generate the content.
   * @param row The number of lines.
   * @throws FileNotFoundException
   * @throws IOException
   */
  public static void generateAsciiFile(String fileName, int row) throws FileNotFoundException, IOException {
    PrintStream output = new PrintStream(fileName);
    output.println(row);
    for (int i = 0; i < row; i++) {
      output.println(i);
    }
    output.close();
  }
}
