package edu.washington.escience.myria.operator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.annotation.Nullable;

import org.apache.commons.lang.BooleanUtils;

import au.com.bytecode.opencsv.CSVParser;
import au.com.bytecode.opencsv.CSVReader;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Floats;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.DateTimeUtils;

/**
 * Reads data from a file. For CSV files, the default parser follows the RFC 4180 (http://tools.ietf.org/html/rfc4180).
 * However, this operator can be used to scan files with different delimiters, etc.
 * 
 * This operator assumes the input file is be comma-separated CSV files and have one record per line. For input files in
 * other formats, delimiter need to be specified, e.g `\t` for tab delimited file, '|' for pipe delimited file. Each
 * cell of the input can be enclosed by the default quotation mark '"'. Other quotation mark like '\'' can be specified
 * by user as well. Note that the enclosure by quotation is not required in the input file.
 * 
 */
public final class FileScan extends LeafOperator {
  /** The Schema of the relation stored in this file. */
  private final Schema schema;
  /** Scanner used to parse the file. */
  private transient CSVReader scanner = null;
  /** A user-provided file delimiter; if null, the system uses the default comma as delimiter. */
  private final Character delimiter;
  /** A user-provided quotation mark, if null, the system uses '"'. */
  private final Character quote;
  /** A user-provided escape character to escape quote and itself, if null, the system uses '/'. */
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
   * Construct a new FileScan object to read from the specified file. This file is assumed to be comma-separated and
   * have one record per line. '"' will be used as default quotation mark. `\` will be used as escape character.
   * 
   * @param filename file containing the data to be scanned.
   * @param schema the Schema of the relation contained in the file.
   */
  public FileScan(final String filename, final Schema schema) {
    this(filename, schema, null, null, null, null);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be comma-separated and
   * have one record per line. '"' will be used as default quotation mark. `\` will be used as escape character.
   * 
   * @param source the data source containing the relation.
   * @param schema the Schema of the relation contained in the file.
   */
  public FileScan(final DataSource source, final Schema schema) {
    this(source, schema, null, null, null, null);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be comma-separated and
   * have one record per line. If delimiter is non-null, the system uses its value as a delimiter. '"' will be used as
   * default quotation mark. `\` will be used as escape character.
   * 
   * @param filename file containing the data to be scanned.
   * @param schema the Schema of the relation contained in the file.
   * @param delimiter An optional override file delimiter.
   */
  public FileScan(final String filename, final Schema schema, final Character delimiter) {
    this(new FileSource(filename), schema, delimiter, null, null, null);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be comma-separated and
   * have one record per line. If delimiter is non-null, the system uses its value as a delimiter. '"' will be used as
   * default quotation mark. `\` will be used as escape character.
   * 
   * @param source file containing the data to be scanned.
   * @param schema the Schema of the relation contained in the file.
   * @param delimiter An optional override file delimiter.
   */
  public FileScan(final DataSource source, final Schema schema, final Character delimiter) {
    this(source, schema, delimiter, null, null, null);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be comma-separated and
   * have one record per line. If delimiter is non-null, the system uses its value as a delimiter. If quote is null, '"'
   * will be used as default quotation mark. If escape is null, `\` will be used as escape character. If
   * numberOfSkippedLines is null, no line will be skipped.
   * 
   * @param filename file containing the data to be scanned.
   * @param schema the Schema of the relation contained in the file.
   * @param delimiter An optional override file delimiter.
   * @param quote An optional quote character
   * @param escape An optional escape character.
   * @param numberOfSkippedLines number of lines to be skipped.
   */
  public FileScan(final String filename, final Schema schema, @Nullable final Character delimiter,
      @Nullable final Character quote, @Nullable final Character escape, @Nullable final Integer numberOfSkippedLines) {
    this(new FileSource(filename), schema, delimiter, quote, escape, numberOfSkippedLines);
  }

  /**
   * Construct a new FileScan object to read from the specified file. This file is assumed to be comma-separated and
   * have one record per line. If delimiter is non-null, the system uses its value as a delimiter. If quote is null, '"'
   * will be used as default quotation mark. If escape is null, `\` will be used as escape character. If
   * numberOfSkippedLines is null, no line will be skipped.
   * 
   * @param source the data source containing the relation.
   * @param schema the Schema of the relation contained in the file.
   * @param delimiter An optional override file delimiter.
   * @param quote An optional quote character
   * @param escape An optional escape character.
   * @param numberOfSkippedLines number of lines to be skipped (number of lines in header).
   */
  public FileScan(final DataSource source, final Schema schema, final Character delimiter,
      @Nullable final Character quote, final Character escape, final Integer numberOfSkippedLines) {
    this.source = Preconditions.checkNotNull(source, "source");
    this.schema = Preconditions.checkNotNull(schema, "schema");
    this.delimiter = Objects.firstNonNull(delimiter, CSVParser.DEFAULT_SEPARATOR);
    this.quote = Objects.firstNonNull(quote, CSVParser.DEFAULT_QUOTE_CHARACTER);
    this.escape = Objects.firstNonNull(escape, CSVParser.DEFAULT_ESCAPE_CHARACTER);
    this.numberOfSkippedLines = Objects.firstNonNull(numberOfSkippedLines, CSVReader.DEFAULT_SKIP_LINES);
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
        throw new DbException("Error parsing row " + lineNumber + ": Found " + nextLine.length
            + " column(s) but expected " + schema.numColumns() + " column(s).");
      }
      for (int column = 0; column < schema.numColumns(); ++column) {
        try {
          switch (schema.getColumnType(column)) {
            case BOOLEAN_TYPE:
              if (Floats.tryParse(nextLine[column]) != null) {
                buffer.putBoolean(column, Floats.tryParse(nextLine[column]) != 0);
              } else if (BooleanUtils.toBoolean(nextLine[column])) {
                buffer.putBoolean(column, Boolean.parseBoolean(nextLine[column]));
              }
              break;
            case DOUBLE_TYPE:
              buffer.putDouble(column, Double.parseDouble(nextLine[column]));
              break;
            case FLOAT_TYPE:
              buffer.putFloat(column, Float.parseFloat(nextLine[column]));
              break;
            case INT_TYPE:
              buffer.putInt(column, Integer.parseInt(nextLine[column]));
              break;
            case LONG_TYPE:
              buffer.putLong(column, Long.parseLong(nextLine[column]));
              break;
            case STRING_TYPE:
              buffer.putString(column, nextLine[column]);
              break;
            case DATETIME_TYPE:
              buffer.putDateTime(column, DateTimeUtils.parse(nextLine[column]));
              break;
          }
        } catch (final IllegalArgumentException e) {
          throw new DbException("Error parsing column " + column + " of row " + lineNumber + ", expected type: "
              + schema.getColumnType(column) + ", scanned value: " + nextLine[column], e);
        }
      }
    }

    LOGGER.debug("Scanned {} input lines", lineNumber - lineNumberBegin);

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
      scanner =
          new CSVReader(new BufferedReader(new InputStreamReader(source.getInputStream())), delimiter, quote, escape,
              numberOfSkippedLines);

    } catch (IOException e) {
      throw new DbException(e);
    }

    lineNumber = 0;
  }
}
