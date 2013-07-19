package edu.washington.escience.myriad.operator;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.InputMismatchException;
import java.util.Objects;
import java.util.Scanner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.util.DateTimeUtils;

/**
 * Reads data from a file.
 * 
 * @author dhalperi
 * 
 */
public final class FileScan extends LeafOperator {
  /** The input stream that this scan is reading from. */
  private transient InputStream inputStream = null;
  /** The Schema of the relation stored in this file. */
  private final Schema schema;
  /** Scanner used to parse the file. */
  private transient Scanner scanner = null;
  /** Whether a comma is a delimiter in this file. */
  private final boolean commaIsDelimiter;

  /** The filename of the input, if any. */
  private String filename = null;

  /** Holds the tuples that are ready for release. */
  private transient TupleBatchBuffer buffer;
  /** Which line of the file the scanner is currently on. */
  private int lineNumber = 0;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line.
   * 
   * @param filename the file containing the relation.
   * @param schema the Schema of the relation contained in the file.
   * @throws FileNotFoundException if the given filename does not exist.
   */
  public FileScan(final String filename, final Schema schema) throws FileNotFoundException {
    this(filename, schema, false);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line. If commaIsDelimiter is true, then records may be whitespace or comma-separated.
   * 
   * @param filename the file containing the relation.
   * @param schema the Schema of the relation contained in the file.
   * @param commaIsDelimiter whether commas are also delimiters in the file.
   * @throws FileNotFoundException if the given filename does not exist.
   */
  public FileScan(final String filename, final Schema schema, final boolean commaIsDelimiter)
      throws FileNotFoundException {
    Objects.requireNonNull(filename);
    Objects.requireNonNull(schema);
    this.schema = schema;
    this.commaIsDelimiter = commaIsDelimiter;
    this.filename = filename;
  }

  /**
   * Construct a new FileScan object to read from a inputStream. If commaIsDelimiter is true, then records may be
   * whitespace or comma-separated. inputStream is assumed to be set later by setInputStream().
   * 
   * @param schema the Schema of the relation contained in the file.
   * @param commaIsDelimiter whether commas are also delimiters in the file.
   */
  public FileScan(final Schema schema, final boolean commaIsDelimiter) {
    this.schema = schema;
    this.commaIsDelimiter = commaIsDelimiter;
  }

  /**
   * Construct a new FileScan object to read from a input stream. The data is assumed to be whitespace-separated and
   * have one record per line. inputStream is assumed to be set later by setInputStream().
   * 
   * @param schema the Schema of the relation contained in the file.
   */
  public FileScan(final Schema schema) {
    this(schema, false);
  }

  /**
   * @param inputStream the data containing the relation.
   */
  public void setInputStream(final InputStream inputStream) {
    this.inputStream = inputStream;
  }

  @Override
  public void cleanup() {
    scanner = null;
    while (buffer.numTuples() > 0) {
      buffer.popAny();
    }
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    /* Let's assume that the scanner always starts at the beginning of a line. */
    while (scanner.hasNext() && (buffer.numTuples() < TupleBatch.BATCH_SIZE)) {
      lineNumber++;
      for (int count = 0; count < schema.numColumns(); ++count) {
        /* Make sure the schema matches. */
        try {
          switch (schema.getColumnType(count)) {
            case BOOLEAN_TYPE:
              if (scanner.hasNextBoolean()) {
                buffer.put(count, scanner.nextBoolean());
              } else if (scanner.hasNextFloat()) {
                buffer.put(count, scanner.nextFloat() != 0);
              }
              break;
            case DOUBLE_TYPE:
              buffer.put(count, scanner.nextDouble());
              break;
            case FLOAT_TYPE:
              buffer.put(count, scanner.nextFloat());
              break;
            case INT_TYPE:
              buffer.put(count, scanner.nextInt());
              break;
            case LONG_TYPE:
              buffer.put(count, scanner.nextLong());
              break;
            case STRING_TYPE:
              buffer.put(count, scanner.next());
              break;
            case DATETIME_TYPE:
              buffer.put(count, DateTimeUtils.parse(scanner.next()));
              break;
          }
        } catch (final InputMismatchException e) {
          throw new DbException("Error parsing column " + count + " of row " + lineNumber + ": ", e);
        }
      }
      final String rest = scanner.nextLine().trim();
      if (rest.length() > 0) {
        throw new DbException("Unexpected output at the end of line " + lineNumber + ": " + rest);
      }
    }
    return buffer.popAny();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    buffer = new TupleBatchBuffer(getSchema());
    if (filename != null) {
      try {
        inputStream = new FileInputStream(filename);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
        return;
      }
    }
    Preconditions.checkArgument(inputStream != null, "FileScan input stream has not been set!");
    scanner = new Scanner(new BufferedReader(new InputStreamReader(inputStream)));
    if (commaIsDelimiter) {
      scanner.useDelimiter("[(\\r\\n)\\n,]");
    }
    lineNumber = 0;
  }
}
