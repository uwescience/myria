package edu.washington.escience.myria.operator;

import com.google.common.collect.ImmutableList;
import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.io.ByteArraySource;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.storage.TupleBatch;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * To test BinaryFileScan, and it is based on the code from FileScanTest
 *
 * @author leelee
 *
 */
public class BinaryFileScanTest {

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
    BinaryFileScan bfs = new BinaryFileScan(schema, new FileSource(filename));
    assertEquals(2, getRowCount(bfs));
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
    BinaryFileScan bfs = new BinaryFileScan(schema, new FileSource(filename));
    assertEquals(8, getRowCount(bfs));
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
    BinaryFileScan bfs = new BinaryFileScan(schema, new FileSource(filename), true);
    assertEquals(1291, getRowCount(bfs));
  }

  @Test
  public void testGenerateReadBinary() throws Exception {
    Schema schema = new Schema(ImmutableList.of(  // one of each
        Type.BOOLEAN_TYPE, Type.DOUBLE_TYPE, Type.FLOAT_TYPE, Type.INT_TYPE,
        Type.LONG_TYPE, Type.STRING_TYPE
    ));
    byte[] buf;
    {
      ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);
      DataOutputStream stream = new DataOutputStream(bos);
      generateBinaryData(stream, schema.getColumnTypes().toArray(new Type[0]), 10);
      stream.close();
      buf = bos.toByteArray();
    }

    BinaryFileScan bfs = new BinaryFileScan(schema, new ByteArraySource(buf));
    assertEquals(10, getRowCount(bfs));
  }

  /**
   * Generates a binary file with the given file name, type array and the number of rows.
   */
  @SuppressWarnings("unused")
  private void generateBinaryFile(String filename, Type[] typeAr, int numrows) {
    try (DataOutputStream raf = new DataOutputStream(new FileOutputStream(filename))) {
      generateBinaryData(raf, typeAr, numrows);
    } catch (IOException e) {
      throw new RuntimeException("", e);
    }
  }

  /**
   * Write binary data to a stream, for given types and number of rows.
   *
   * @param stream The data stream to write data to. Does not close the stream.
   */
  @SuppressWarnings("unused")
  private void generateBinaryData(DataOutputStream stream, Type[] typeAr, int numrows) throws IOException {
    for (int i = 0; i < numrows; i++) {
      for (Type element : typeAr) {
        switch (element) {
          case BOOLEAN_TYPE:
            stream.writeBoolean(true);
            break;
          case DOUBLE_TYPE:
            stream.writeDouble(i);
            break;
          case FLOAT_TYPE:
            stream.writeFloat(i);
            break;
          case INT_TYPE:
            stream.writeInt(i);
            break;
          case LONG_TYPE:
            stream.writeLong(i);
            break;
          case STRING_TYPE:
            stream.writeUTF("string"+i);
            break;
          default:
            throw new UnsupportedOperationException(
                "can only write fix length field to bin file");
        }
      }
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
  private void generateSimpleBinaryFile(String filename, int row) {
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
  private static int getRowCount(BinaryFileScan fileScan) throws DbException {
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
}
