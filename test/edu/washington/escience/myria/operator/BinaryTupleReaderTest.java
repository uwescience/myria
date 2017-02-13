package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.BinaryTupleReader;
import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.util.TestEnvVars;

/**
 * To test BinaryFileScan, and it is based on the code from FileScanTest
 *
 * @author leelee
 *
 */
public class BinaryTupleReaderTest {

  @Test
  /**
   * Test default BinaryFileScan that reads data bytes in big endian format.
   *
   * File was generated with:
   *     generateSimpleBinaryFile(filename, 2);
   */
  public void testSimple() throws DbException {
    Schema schema = new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE));
    String filename =
        "testdata" + File.separatorChar + "binaryfilescan" + File.separatorChar + "testSimple";
    TupleSource ts = new TupleSource(new BinaryTupleReader(schema), new FileSource(filename));
    assertEquals(2, getRowCount(ts));
  }

  @Test
  /**
   * Test default BinaryFileScan that reads data bytes in big endian format with the bin file
   * that has the astronomy data schema.
   *
   * File was generated with:
   *     generateSimpleBinaryFile(filename, 2);
   */
  public void testWithAstronomySchema() throws DbException {
    Type[] typeAr = {
      Type.LONG_TYPE, // iOrder
      Type.FLOAT_TYPE, // mass
      Type.FLOAT_TYPE, // x
      Type.FLOAT_TYPE, // y
      Type.FLOAT_TYPE, // z
      Type.FLOAT_TYPE, // vx
      Type.FLOAT_TYPE, // vy
      Type.FLOAT_TYPE, // vz
      Type.FLOAT_TYPE, // rho
      Type.FLOAT_TYPE, // temp
      Type.FLOAT_TYPE, // hsmooth
      Type.FLOAT_TYPE, // metals
      Type.FLOAT_TYPE, // tform
      Type.FLOAT_TYPE, // eps
      Type.FLOAT_TYPE // phi
    };
    Schema schema = new Schema(Arrays.asList(typeAr));
    String filename =
        "testdata"
            + File.separatorChar
            + "binaryfilescan"
            + File.separatorChar
            + "testWithAstronomySchema";
    TupleSource ts = new TupleSource(new BinaryTupleReader(schema), new FileSource(filename));
    assertEquals(8, getRowCount(ts));
  }

  @Test
  /**
   * Test BinaryFileScan with the real cosmo data bin file
   */
  public void testNumRowsFromCosmo24Star() throws DbException {
    Type[] typeAr = {
      Type.LONG_TYPE, // iOrder
      Type.FLOAT_TYPE, // mass
      Type.FLOAT_TYPE, // x
      Type.FLOAT_TYPE, // y
      Type.FLOAT_TYPE, // z
      Type.FLOAT_TYPE, // vx
      Type.FLOAT_TYPE, // vy
      Type.FLOAT_TYPE, // vz
      Type.FLOAT_TYPE, // metals
      Type.FLOAT_TYPE, // tform
      Type.FLOAT_TYPE, // eps
      Type.FLOAT_TYPE, // phi
    };
    Schema schema = new Schema(Arrays.asList(typeAr));
    String filename =
        "testdata"
            + File.separatorChar
            + "binaryfilescan"
            + File.separatorChar
            + "cosmo50cmb.256g2bwK.00024.star.bin";
    TupleSource ts = new TupleSource(new BinaryTupleReader(schema, true), new FileSource(filename));
    assertEquals(1291, getRowCount(ts));
  }

  @Test
  public void testGenerateReadBinary() throws Exception {
    Schema schema =
        new Schema(
            ImmutableList.of( // one of each
                Type.BOOLEAN_TYPE,
                Type.DOUBLE_TYPE,
                Type.FLOAT_TYPE,
                Type.INT_TYPE,
                Type.LONG_TYPE,
                Type.STRING_TYPE));
    File file = File.createTempFile(this.getClass().getSimpleName(), null);
    String filename = file.getCanonicalPath();
    generateBinaryFile(filename, schema.getColumnTypes().toArray(new Type[0]), 10);
    TupleSource ts = new TupleSource(new BinaryTupleReader(schema, true), new FileSource(filename));
    assertEquals(10, getRowCount(ts));
  }

  /**
   * Generates a binary file with the given file name, type array and the number of rows.
   *
   * @param filename The filename to create.
   * @param typeAr The array of types.
   * @param row The number of rows.
   */
  @SuppressWarnings("unused")
  private void generateBinaryFile(final String filename, final Type[] typeAr, final int row) {
    try {
      RandomAccessFile raf = new RandomAccessFile(filename, "rw");
      for (int i = 0; i < row; i++) {
        for (Type element : typeAr) {
          switch (element) {
            case BOOLEAN_TYPE:
              raf.writeBoolean(true);
              break;
            case DOUBLE_TYPE:
              raf.writeDouble(i);
              break;
            case FLOAT_TYPE:
              raf.writeFloat(i);
              break;
            case INT_TYPE:
              raf.writeInt(i);
              break;
            case LONG_TYPE:
              raf.writeLong(i);
              break;
            case STRING_TYPE:
              raf.writeUTF("string" + i);
              break;
            default:
              throw new UnsupportedOperationException(
                  "cannot write field of type " + element.getName() + " to binary file");
          }
        }
      }
      raf.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Generates a simple binary file with the given file name with the given number of row. The generated binary file
   * will contains two int in each row.
   *
   * @param filename
   * @param row
   */
  @SuppressWarnings("unused")
  private void generateSimpleBinaryFile(final String filename, final int row) {
    try {
      RandomAccessFile raf = new RandomAccessFile(filename, "rw");
      for (int i = 0; i < row; i++) {
        raf.writeInt(i);
        raf.writeInt(i);
      }
      raf.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Helper function used to run tests.
   *
   * @param fileScan the FileScan object to be tested.
   * @return the number of rows in the file.
   * @throws DbException if the file does not match the given Schema.
   */
  private static int getRowCount(final TupleSource input) throws DbException {
    input.open(TestEnvVars.get());

    int count = 0;
    TupleBatch tb = null;
    while (!input.eos()) {
      tb = input.nextReady();
      if (tb != null) {
        count += tb.numTuples();
      }
    }
    return count;
  }
}
