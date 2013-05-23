package edu.washington.escience.myriad.operator;

import static org.junit.Assert.assertEquals;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.junit.Test;

import com.google.common.io.LittleEndianDataOutputStream;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.SimplePredicate;
import edu.washington.escience.myriad.TupleBatch;

/**
 * 
 * @author leelee
 * 
 */
public class TipsyFileScanTest {

  private static final int NUM_COLUMN_GAS = 12;
  private static final int NUM_COLUMN_DARK = 9;
  private static final int NUM_COLUMN_STAR = 11;

  private static int getRowCount(Operator fileScan) throws DbException {
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
  public void testSimpleLittleEndian() throws IOException, DbException {
    String binFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "tipsy2";
    String iOrderFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "iOrder2.iord";
    String grpFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "grp2.amiga.grp";
    TipsyFileScan filescan = new TipsyFileScan(binFilename, iOrderFilename, grpFilename, true);
    assertEquals(3, getRowCount(filescan));
  }

  @Test
  public void testSimpleBigEndianStar() throws IOException, DbException {
    String binFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "tipsy3";
    String iOrderFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "iOrder3.iord";
    String grpFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "grp3.amiga.grp";
    TipsyFileScan filescan = new TipsyFileScan(binFilename, iOrderFilename, grpFilename, true);
    Filter filter = new Filter(new SimplePredicate(16, SimplePredicate.Op.EQUALS, "star"), filescan);
    assertEquals(3, getRowCount(filter));
  }

  @Test
  public void testSimpleBigEndianDark() throws IOException, DbException {
    String binFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "tipsy3";
    String iOrderFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "iOrder3.iord";
    String grpFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "grp3.amiga.grp";
    TipsyFileScan filescan = new TipsyFileScan(binFilename, iOrderFilename, grpFilename, true);
    Filter filter = new Filter(new SimplePredicate(16, SimplePredicate.Op.EQUALS, "dark"), filescan);
    assertEquals(2, getRowCount(filter));
  }

  @Test
  public void testSimpleBigEndianGas() throws IOException, DbException {
    String binFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "tipsy3";
    String iOrderFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "iOrder3.iord";
    String grpFilename = "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "grp3.amiga.grp";
    TipsyFileScan filescan = new TipsyFileScan(binFilename, iOrderFilename, grpFilename, true);
    Filter filter = new Filter(new SimplePredicate(16, SimplePredicate.Op.EQUALS, "gas"), filescan);
    assertEquals(4, getRowCount(filter));
  }

  // @Test
  // warning: this test takes about 35 minutes to run on a mac with 8gb ram, i7
  // processor.
  public void testCosmo512Star() throws DbException {
    String binFileName =
        "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "cosmo50cmb.256g2MbwK.00512";
    String iOrderFileName =
        "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "cosmo50cmb.256g2MbwK.00512.iord";
    String grpFileName =
        "testdata" + File.separatorChar + "tipsyfilescan" + File.separatorChar + "cosmo50cmb.256g2MbwK.00512.amiga.grp";
    TipsyFileScan tfScan = new TipsyFileScan(binFileName, iOrderFileName, grpFileName);
    Filter filter = new Filter(new SimplePredicate(16, SimplePredicate.Op.EQUALS, "star"), tfScan);

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
  public static void generateBigEndianBinaryFile(String filename, int ngas, int ndark, int nstar) throws IOException {
    FileOutputStream fStream = new FileOutputStream(filename);
    DataOutputStream output = new DataOutputStream(fStream);
    generateBinaryFile(output, ngas, ndark, nstar);
  }

  /**
   * Generate little endian binary file.
   * 
   * @param filename The filename to generate the content.
   * @param ngas The number of gas records.
   * @param ndark The number of dark records.
   * @param nstar The number of star records.
   * @throws IOException if error writing to file.
   */
  public static void generateLittleEndianBinaryFile(String filename, int ngas, int ndark, int nstar) throws IOException {
    FileOutputStream fStream = new FileOutputStream(filename);
    LittleEndianDataOutputStream output = new LittleEndianDataOutputStream(fStream);
    generateBinaryFile(output, ngas, ndark, nstar);
  }

  /**
   * Generate binary file using the DataOutput object.
   * 
   * @param output The DataOutput object.
   * @param ngas The number of gas records.
   * @param ndark The number of dark records.
   * @param nstar The number of star records.
   * @throws IOException if error writing to file.
   */
  private static void generateBinaryFile(DataOutput output, int ngas, int ndark, int nstar) throws IOException {
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
