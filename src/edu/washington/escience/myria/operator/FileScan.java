package edu.washington.escience.myria.operator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.commons.lang.BooleanUtils;

import au.com.bytecode.opencsv.CSVReader;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Floats;

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
  private transient CSVReader scanner = null;
  /** A user-provided file delimiter; if null, the system uses the default whitespace delimiter. */
  private final Character delimiter;
  /** A user-provided escape character, if null, the system uses '/'. */
  private final Character escape;
  /** The data source that will generate the input stream to be read at initialization. */
  private final DataSource source;
  /** Number of skipped lines on the head. */
  private final Integer numberOfSkippedLines;
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
    this(filename, schema, null, null, null);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line.
   * 
   * @param source the data source containing the relation.
   * @param schema the Schema of the relation contained in the file.
   */
  public FileScan(final DataSource source, final Schema schema) {
    this(source, schema, null, null, null);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line.
   * 
   * @param filename file containing the data to be scanned.
   * @param schema the Schema of the relation contained in the file.
   * @param delimiter An optional override file delimiter.
   */
  public FileScan(final String filename, final Schema schema, final Character delimiter) {
    this(filename, schema, delimiter, null, null);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line.
   * 
   * @param source the data source containing the relation.
   * @param schema the Schema of the relation contained in the file.
   * @param delimiter An optional override file delimiter.
   */
  public FileScan(final DataSource source, final Schema schema, final Character delimiter) {
    this(source, schema, delimiter, null, null);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line.
   * 
   * @param filename file containing the data to be scanned.
   * @param schema the Schema of the relation contained in the file.
   * @param delimiter An optional override file delimiter.
   * @param escape An optional escape character.
   */
  public FileScan(final String filename, final Schema schema, final Character delimiter, final Character escape) {
    this(filename, schema, delimiter, escape, null);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line. If delimiter is non-null, the system uses its value as a delimiter.
   * 
   * @param source the data source containing the relation.
   * @param schema the Schema of the relation contained in the file.
   * @param delimiter An optional override file delimiter.
   * @param escape An optional escape character.
   */
  public FileScan(final DataSource source, final Schema schema, final Character delimiter, final Character escape) {
    this(source, schema, delimiter, escape, null);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line. If delimiter is non-null, the system uses its value as a delimiter.
   * 
   * @param filename file containing the data to be scanned.
   * @param schema the Schema of the relation contained in the file.
   * @param delimiter An optional override file delimiter.
   * @param escape An optional escape character.
   * @param numberOfSkippedLines number of lines to be skipped.
   */
  public FileScan(final String filename, final Schema schema, @Nullable final Character delimiter,
      @Nullable final Character escape, @Nullable final Integer numberOfSkippedLines) {
    this(new FileSource(filename), schema, delimiter, escape, numberOfSkippedLines);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be whitespace-separated
   * and have one record per line. If delimiter is non-null, the system uses its value as a delimiter.
   * 
   * @param source the data source containing the relation.
   * @param schema the Schema of the relation contained in the file.
   * @param delimiter An optional override file delimiter.
   * @param escape An optional escape character.
   * @param numberOfSkippedLines number of lines to be skipped.
   */
  public FileScan(final DataSource source, final Schema schema, final Character delimiter, final Character escape,
      final Integer numberOfSkippedLines) {
    this.source = Objects.requireNonNull(source);
    this.schema = Objects.requireNonNull(schema);
    this.delimiter = delimiter;
    this.escape = escape;
    this.numberOfSkippedLines = numberOfSkippedLines;
  }

  @Override
  public void cleanup() {
    scanner = null;
    while (buffer.numTuples() > 0) {
      buffer.popAny();
    }
  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException, IOException {
    /* Let's assume that the scanner always starts at the beginning of a line. */
    int lineNumberBegin = lineNumber;

    while ((buffer.numTuples() < TupleBatch.BATCH_SIZE)) {
      String[] nextLine = scanner.readNext();
      if (nextLine == null) {
        break;
      }
      lineNumber++;
      if (nextLine.length != schema.numColumns()) {
        throw new DbException("Error parsing row " + lineNumber + ": unexpected number of columns " + nextLine.length
            + "(correct:" + schema.numColumns() + ")");
      }
      for (int count = 0; count < schema.numColumns(); ++count) {
        /* Make sure the schema matches. */
        try {
          switch (schema.getColumnType(count)) {
            case BOOLEAN_TYPE:
              if (Floats.tryParse(nextLine[count]) != null) {
                buffer.putBoolean(count, Floats.tryParse(nextLine[count]) != 0);
              } else if (BooleanUtils.toBoolean(nextLine[count])) {
                buffer.putBoolean(count, Boolean.parseBoolean(nextLine[count]));
              }
              break;
            case DOUBLE_TYPE:
              buffer.putDouble(count, Double.parseDouble(nextLine[count]));
              break;
            case FLOAT_TYPE:
              buffer.putFloat(count, Float.parseFloat(nextLine[count]));
              break;
            case INT_TYPE:
              buffer.putInt(count, Integer.parseInt(nextLine[count]));
              break;
            case LONG_TYPE:
              buffer.putLong(count, Long.parseLong(nextLine[count]));
              break;
            case STRING_TYPE:
              buffer.putString(count, nextLine[count]);
              break;
            case DATETIME_TYPE:
              buffer.putDateTime(count, DateTimeUtils.parse(nextLine[count]));
              break;
          }
        } catch (final IllegalArgumentException e) {
          throw new DbException("Error parsing column " + count + " of row " + lineNumber + ": ", e);
        }
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
    Character delim = ',';
    Character esc = '\\';
    Integer skip = 0;

    if (delimiter != null) {
      delim = delimiter;
    }

    if (escape != null) {
      esc = escape;
    }

    if (numberOfSkippedLines != null) {
      skip = numberOfSkippedLines;
    }

    try {
      scanner =
          new CSVReader(new BufferedReader(new InputStreamReader(source.getInputStream())), delim, '"', esc, skip);

    } catch (IOException e) {
      throw new DbException(e);
    }

    lineNumber = 0;
  }
}
