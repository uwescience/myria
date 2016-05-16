package edu.washington.escience.myria.operator;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * Read and merge Tipsy bin file, iOrder ascii file and group number ascii file.
 *
 * @author leelee
 *
 */
public class TipsyFileScan extends LeafOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The header size in bytes. */
  private static final int H_SIZE = 32;
  /** The gas record size in bytes. */
  private static final int G_SIZE = 48;
  /** The dark record size in bytes. */
  private static final int D_SIZE = 36;
  /** The star record size in bytes. */
  private static final int S_SIZE = 44;
  /** The data input for bin file. */
  private transient DataInput dataInputForBin;
  /** Scanner used to parse the iOrder file. */
  private transient Scanner iOrderScanner = null;
  /** Scanner used to parse the group number file. */
  private transient Scanner grpScanner = null;
  /** Holds the tuples that are ready for release. */
  private transient TupleBatchBuffer buffer;

  /** The bin file name. */
  private final String binFileName;
  /** The iOrder file name. */
  private final String iOrderFileName;
  /** The group number file name. */
  private final String grpFileName;
  /** The number of gas particle record. */
  private long ngas;
  /** The number of star particle record. */
  private long nstar;
  /** The number of dark particle record. */
  private long ndark;
  /** Which line of the file the scanner is currently on. */
  private int lineNumber;

  /** Schema for all Tipsy files. */
  private static final Schema TIPSY_SCHEMA =
      new Schema(
          ImmutableList.of(
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
              Type.FLOAT_TYPE, // phi
              Type.INT_TYPE, // grp
              Type.STRING_TYPE // type
              ),
          ImmutableList.of(
              "iOrder", "mass", "x", "y", "z", "vx", "vy", "vz", "rho", "temp", "hsmooth", "metals",
              "tform", "eps", "phi", "grp", "type"));

  /**
   * Construct a new TipsyFileScan object using the given binary filename, iOrder filename and group number filename. By
   * default TipsyFileScan will read the given binary file in big endian format.
   *
   * @param binFileName The binary file that contains the data for gas, dark, star particles.
   * @param iOrderFileName The ascii file that contains the data for iOrder.
   * @param grpFileName The ascii file that contains the data for group number.
   */
  public TipsyFileScan(
      final String binFileName, final String iOrderFileName, final String grpFileName) {
    Objects.requireNonNull(binFileName);
    Objects.requireNonNull(iOrderFileName);
    Objects.requireNonNull(grpFileName);
    this.binFileName = binFileName;
    this.iOrderFileName = iOrderFileName;
    this.grpFileName = grpFileName;
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    processGasRecords();
    processDarkRecords();
    processStarRecords();
    return buffer.popAny();
  }

  @Override
  protected final void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    buffer = new TupleBatchBuffer(getSchema());
    InputStream iOrderInputStream = openFileOrUrlInputStream(iOrderFileName);
    InputStream grpInputStream = openFileOrUrlInputStream(grpFileName);
    int ntot;

    try {
      // Create a fileInputStream for the bin file
      InputStream fStreamForBin = openFileOrUrlInputStream(binFileName);
      BufferedInputStream bufferedStreamForBin = new BufferedInputStream(fStreamForBin);
      dataInputForBin = new DataInputStream(bufferedStreamForBin);

      dataInputForBin.readDouble(); // time
      ntot = dataInputForBin.readInt();
      dataInputForBin.readInt();
      ngas = dataInputForBin.readInt();
      ndark = dataInputForBin.readInt();
      nstar = dataInputForBin.readInt();
      dataInputForBin.readInt();
      long proposed = H_SIZE + ngas * G_SIZE + ndark * D_SIZE + nstar * S_SIZE;
      if (ntot != ngas + ndark + nstar) {
        throw new DbException("header info incorrect");
      }
      if (fStreamForBin instanceof FileInputStream
          && proposed != ((FileInputStream) fStreamForBin).getChannel().size()) {
        throw new DbException("binary file size incorrect");
      }
    } catch (IOException e) {
      throw new DbException(e);
    }

    Preconditions.checkArgument(
        iOrderInputStream != null, "FileScan iOrder input stream has not been set!");
    Preconditions.checkArgument(
        grpInputStream != null, "FileScan group input stream has not been set!");
    Preconditions.checkArgument(
        dataInputForBin != null, "FileScan binary input stream has not been set!");
    iOrderScanner = new Scanner(new BufferedReader(new InputStreamReader(iOrderInputStream)));
    grpScanner = new Scanner(new BufferedReader(new InputStreamReader(grpInputStream)));
    int numIOrder = iOrderScanner.nextInt();
    int numGrp = grpScanner.nextInt();
    if (numIOrder != ntot) {
      throw new DbException(
          "number of iOrder "
              + numIOrder
              + " is different from the number of tipsy record "
              + ntot
              + ".");
    }
    if (numGrp != ntot) {
      throw new DbException("number of group is different from the number of tipsy record.");
    }
    lineNumber = 0;
  }

  @Override
  protected final void cleanup() throws DbException {
    iOrderScanner = null;
    grpScanner = null;
    while (buffer.numTuples() > 0) {
      buffer.popAny();
    }
  }

  /**
   * Construct tuples for gas particle records. The expected gas particles schema in the bin file is mass, x, y, z, vx,
   * vy, vz, rho, temp, hsmooth, metals, phi. Merge the record in the binary file with iOrder and group number and fill
   * in the each tuple column accordingly.
   *
   * @throws DbException if error reading from file.
   */
  private void processGasRecords() throws DbException {
    while (ngas > 0 && (buffer.numTuples() < TupleBatch.BATCH_SIZE)) {
      lineNumber++;
      try {
        int count = 0;
        buffer.putLong(count++, iOrderScanner.nextLong());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        /*
         * TODO(leelee): Should be null for the next two columns. Put 0 for now as TupleBatchBuffer does not support
         * null value.
         */
        buffer.putFloat(count++, 0);
        buffer.putFloat(count++, 0);
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putInt(count++, grpScanner.nextInt());
        buffer.putString(count++, "gas");
      } catch (final IOException e) {
        throw new DbException(e);
      }
      final String iOrderRest = iOrderScanner.nextLine().trim();
      if (iOrderRest.length() > 0) {
        throw new DbException(
            "iOrderFile: Unexpected output at the end of line " + lineNumber + ": " + iOrderRest);
      }
      final String grpRest = grpScanner.nextLine().trim();
      if (grpRest.length() > 0) {
        throw new DbException(
            "grpFile: Unexpected output at the end of line " + lineNumber + ": " + grpRest);
      }
      ngas--;
    }
  }

  /**
   * Construct tuples for gas particle records. The expected dark particles schema in the bin file is mass, x, y, z, vx,
   * vy, vz, eps, phi. Merge the record in the binary file with iOrder and group number and fill in the each tuple
   * column accordingly.
   *
   * @throws DbException if error reading from file.
   */
  private void processDarkRecords() throws DbException {
    while (ndark > 0 && (buffer.numTuples() < TupleBatch.BATCH_SIZE)) {
      lineNumber++;
      try {
        int count = 0;
        buffer.putLong(count++, iOrderScanner.nextLong());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        /*
         * TODO(leelee): Should be null for the next five columns. Put 0 for now as TupleBatchBuffer does not support
         * null value.
         */
        buffer.putFloat(count++, 0);
        buffer.putFloat(count++, 0);
        buffer.putFloat(count++, 0);
        buffer.putFloat(count++, 0);
        buffer.putFloat(count++, 0);
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putInt(count++, grpScanner.nextInt());
        buffer.putString(count++, "dark");
      } catch (final IOException e) {
        throw new DbException(e);
      }
      final String iOrderRest = iOrderScanner.nextLine().trim();
      if (iOrderRest.length() > 0) {
        throw new DbException(
            "iOrderFile: Unexpected output at the end of line " + lineNumber + ": " + iOrderRest);
      }
      final String grpRest = grpScanner.nextLine().trim();
      if (grpRest.length() > 0) {
        throw new DbException(
            "grpFile: Unexpected output at the end of line " + lineNumber + ": " + grpRest);
      }
      ndark--;
    }
  }

  /**
   * Construct tuples for gas particle records. The expected dark particles schema in the bin file is mass, x, y, z, vx,
   * vy, vz, metals, tform, eps, phi. Merge the record in the binary file with iOrder and group number and fill in the
   * each tuple column accordingly.
   *
   * @throws DbException if error reading from file.
   */
  private void processStarRecords() throws DbException {
    while (nstar > 0 && (buffer.numTuples() < TupleBatch.BATCH_SIZE)) {
      lineNumber++;
      try {
        int count = 0;
        buffer.putLong(count++, iOrderScanner.nextLong());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        /*
         * TODO(leelee): Should be null for the next three columns. Put 0 for now as TupleBatchBuffer does not support
         * null value.
         */
        buffer.putFloat(count++, 0);
        buffer.putFloat(count++, 0);
        buffer.putFloat(count++, 0);
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putFloat(count++, dataInputForBin.readFloat());
        buffer.putInt(count++, grpScanner.nextInt());
        buffer.putString(count++, "star");
      } catch (final IOException e) {
        throw new DbException(e);
      }
      final String iOrderRest = iOrderScanner.nextLine().trim();
      if (iOrderRest.length() > 0) {
        throw new DbException(
            "iOrderFile: Unexpected output at the end of line " + lineNumber + ": " + iOrderRest);
      }
      final String grpRest = grpScanner.nextLine().trim();
      if (grpRest.length() > 0) {
        throw new DbException(
            "grpFile: Unexpected output at the end of line " + lineNumber + ": " + grpRest);
      }
      nstar--;
    }
  }

  @Override
  protected Schema generateSchema() {
    return TIPSY_SCHEMA;
  }

  private static InputStream openFileOrUrlInputStream(String filenameOrUrl) throws DbException {
    try {
      URI uri = new URI(filenameOrUrl);
      if (uri.getScheme() == null) {
        return openFileInputStream(filenameOrUrl);
      } else if (uri.getScheme().equals("hdfs")) {
        return openHdfsInputStream(uri);
      } else {
        return uri.toURL().openStream();
      }
    } catch (IllegalArgumentException e) {
      return openFileInputStream(filenameOrUrl);
    } catch (URISyntaxException e) {
      return openFileInputStream(filenameOrUrl);
    } catch (MalformedURLException e) {
      return openFileInputStream(filenameOrUrl);
    } catch (IOException e) {
      throw new DbException(e);
    }
  }

  private static InputStream openFileInputStream(String filename) throws DbException {
    try {
      return new FileInputStream(filename);
    } catch (FileNotFoundException e) {
      throw new DbException(e);
    }
  }

  private static InputStream openHdfsInputStream(final URI uri) throws DbException {
    try {
      FileSystem fs = FileSystem.get(uri, new Configuration());
      Path path = new Path(uri);
      return fs.open(path);
    } catch (IOException e) {
      throw new DbException(e);
    }
  }
}
