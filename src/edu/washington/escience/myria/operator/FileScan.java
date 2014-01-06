package edu.washington.escience.myria.operator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Scanner;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.TupleBatchBuffer;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.util.DateTimeUtils;

/**
 * Reads data from a file.
 * 
 * @author dhalperi
 * 
 */
public final class FileScan extends LeafOperator {
  /** The Schema of the relation stored in this file. */
  private final Schema schema;
  /** Scanner used to parse the file. */
  private transient Scanner scanner = null;
  /** A user-provided file delimiter; if null, the system uses the default whitespace delimiter. */
  private final String delimiter;
  /** The data source that will generate the input stream to be read at initialization. */
  private final DataSource source;

  /** Holds the tuples that are ready for release. */
  private transient TupleBatchBuffer buffer;
  /** Which line of the file the scanner is currently on. */
  private int lineNumber = 0;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The logger for debug, trace, etc. messages in this class.
   */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(FileScan.class);

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line.
   * 
   * @param filename file containing the data to be scanned.
   * @param schema the Schema of the relation contained in the file.
   */
  public FileScan(final String filename, final Schema schema) {
    this(filename, schema, null);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line.
   * 
   * @param source the data source containing the relation.
   * @param schema the Schema of the relation contained in the file.
   */
  public FileScan(final DataSource source, final Schema schema) {
    this(source, schema, null);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line. If delimiter is non-null, the system uses its value as a delimiter.
   * 
   * @param filename file containing the data to be scanned.
   * @param schema the Schema of the relation contained in the file.
   * @param delimiter An optional override file delimiter.
   */
  public FileScan(final String filename, final Schema schema, @Nullable final String delimiter) {
    this(new FileSource(filename), schema, delimiter);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line. If delimiter is non-null, the system uses its value as a delimiter.
   * 
   * @param source the data source containing the relation.
   * @param schema the Schema of the relation contained in the file.
   * @param delimiter An optional override file delimiter.
   */
  public FileScan(final DataSource source, final Schema schema, final String delimiter) {
    this.source = Objects.requireNonNull(source);
    this.schema = Objects.requireNonNull(schema);
    this.delimiter = delimiter;
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
    int lineNumberBegin = lineNumber;

    while (scanner.hasNext() && (buffer.numTuples() < TupleBatch.BATCH_SIZE)) {
      lineNumber++;

      for (int count = 0; count < schema.numColumns(); ++count) {
        /* Make sure the schema matches. */
        try {
          switch (schema.getColumnType(count)) {
            case BOOLEAN_TYPE:
              if (scanner.hasNextBoolean()) {
                buffer.putBoolean(count, Boolean.parseBoolean(scanner.next()));
              } else if (scanner.hasNextFloat()) {
                buffer.putBoolean(count, Float.parseFloat(scanner.next()) != 0);
              }
              break;
            case DOUBLE_TYPE:
              buffer.putDouble(count, Double.parseDouble(scanner.next()));
              break;
            case FLOAT_TYPE:
              buffer.putFloat(count, Float.parseFloat(scanner.next()));
              break;
            case INT_TYPE:
              buffer.putInt(count, Integer.parseInt(scanner.next()));
              break;
            case LONG_TYPE:
              buffer.putLong(count, Long.parseLong(scanner.next()));
              break;
            case STRING_TYPE:
              buffer.putString(count, scanner.next());
              break;
            case DATETIME_TYPE:
              buffer.putDateTime(count, DateTimeUtils.parse(scanner.next()));
              break;
          }
        } catch (final IllegalArgumentException e) {
          throw new DbException("Error parsing column " + count + " of row " + lineNumber + ": ", e);
        }
      }
      String rest = null;
      try {
        rest = scanner.nextLine().trim();
      } catch (final NoSuchElementException e) {
        rest = "";
      }

      if (rest.length() > 0) {
        throw new DbException("Unexpected output at the end of line " + lineNumber + ": " + rest);
      }
    }

    LOGGER.debug("Scanned " + (lineNumber - lineNumberBegin) + " input lines");

    return buffer.popAny();
  }

  @Override
  public Schema generateSchema() {
    return schema;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    buffer = new TupleBatchBuffer(getSchema());
    try {
      scanner = new Scanner(new BufferedReader(new InputStreamReader(source.getInputStream())));
    } catch (IOException e) {
      throw new DbException(e);
    }
    if (delimiter != null) {
      scanner.useDelimiter("(\\r\\n)|\\n|(\\Q" + delimiter + "\\E)");
    }
    lineNumber = 0;
  }
}
