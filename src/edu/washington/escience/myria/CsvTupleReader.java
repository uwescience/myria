/**
 *
 */
package edu.washington.escience.myria;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

import javax.annotation.Nullable;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.BooleanUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Floats;

import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.storage.TupleUtils;
import edu.washington.escience.myria.util.DateTimeUtils;

/**
 *
 */
public class CsvTupleReader implements TupleReader {
  /** The Schema of the relation stored in this file. */
  @JsonProperty private final Schema schema;
  /** A user-provided file delimiter; if null, the system uses the default comma as delimiter. */
  @JsonProperty private final Character delimiter;
  /** A user-provided quotation mark, if null, the system uses '"'. */
  @JsonProperty private final Character quote;
  /** A user-provided escape character to escape quote and itself, if null, the system uses '/'. */
  @JsonProperty private final Character escape;
  /** Number of skipped lines on the head. */
  @JsonProperty("skip")
  private final Integer numberOfSkippedLines;

  /** Scanner used to parse the file. */
  private transient CSVParser parser = null;
  /** Iterator over CSV records. */
  private transient Iterator<CSVRecord> iterator = null;
  /** Holds the tuples that are ready for release. */
  private transient TupleBatchBuffer buffer;
  /** Which line of the file the scanner is currently on. */
  private long lineNumber = 0;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The logger for debug, trace, etc. messages in this class.
   */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(CsvTupleReader.class);

  public CsvTupleReader(final Schema schema) {
    this(schema, null, null, null, null);
  }

  public CsvTupleReader(final Schema schema, final Character delimiter) {
    this(schema, delimiter, null, null, null);
  }

  public CsvTupleReader(
      @JsonProperty(value = "schema", required = true) final Schema schema,
      @JsonProperty(value = "delimiter", required = false) @Nullable final Character delimiter,
      @JsonProperty(value = "quote", required = false) @Nullable final Character quote,
      @JsonProperty(value = "escape", required = false) @Nullable final Character escape,
      @JsonProperty(value = "numberOfSkippedLines", required = false) @Nullable
      final Integer numberOfSkippedLines) {
    this.schema = Preconditions.checkNotNull(schema, "schema");

    this.delimiter = MoreObjects.firstNonNull(delimiter, CSVFormat.DEFAULT.getDelimiter());
    this.quote = MoreObjects.firstNonNull(quote, CSVFormat.DEFAULT.getQuoteCharacter());
    this.escape = escape != null ? escape : CSVFormat.DEFAULT.getEscapeCharacter();
    this.numberOfSkippedLines = MoreObjects.firstNonNull(numberOfSkippedLines, 0);
  }

  @Override
  public void open(final InputStream stream) throws IOException, DbException {
    buffer = new TupleBatchBuffer(schema);
    try {
      parser =
          new CSVParser(
              new BufferedReader(new InputStreamReader(stream)),
              CSVFormat.newFormat(delimiter).withQuote(quote).withEscape(escape));
      iterator = parser.iterator();
      for (int i = 0; i < numberOfSkippedLines; i++) {
        iterator.next();
      }
    } catch (IOException e) {
      throw new DbException(e);
    }

    lineNumber = 0;
  }

  @Override
  public TupleBatch readTuples() throws IOException, DbException {
    /* Let's assume that the scanner always starts at the beginning of a line. */
    long lineNumberBegin = lineNumber;
    int batchSize = TupleUtils.get_Batch_size(schema);
    while ((buffer.numTuples() < batchSize)) {
      lineNumber++;
      if (parser.isClosed()) {
        break;
      }
      try {
        if (!iterator.hasNext()) {
          parser.close();
          break;
        }
      } catch (final RuntimeException e) {
        throw new DbException("Error parsing row " + lineNumber, e);
      }
      CSVRecord record = iterator.next();

      if (record.size() != schema.numColumns()) {
        throw new DbException(
            "Error parsing row "
                + lineNumber
                + ": Found "
                + record.size()
                + " column(s) but expected "
                + schema.numColumns()
                + " column(s).");
      }
      for (int column = 0; column < schema.numColumns(); ++column) {
        String cell = record.get(column);
        try {
          switch (schema.getColumnType(column)) {
            case BOOLEAN_TYPE:
              if (Floats.tryParse(cell) != null) {
                buffer.putBoolean(column, Floats.tryParse(cell) != 0);
              } else if (BooleanUtils.toBoolean(cell)) {
                buffer.putBoolean(column, Boolean.parseBoolean(cell));
              }
              break;
            case DOUBLE_TYPE:
              buffer.putDouble(column, Double.parseDouble(cell));
              break;
            case FLOAT_TYPE:
              buffer.putFloat(column, Float.parseFloat(cell));
              break;
            case INT_TYPE:
              buffer.putInt(column, Integer.parseInt(cell));
              break;
            case LONG_TYPE:
              buffer.putLong(column, Long.parseLong(cell));
              break;
            case STRING_TYPE:
              buffer.putString(column, cell);
              break;
            case DATETIME_TYPE:
              buffer.putDateTime(column, DateTimeUtils.parse(cell));
              break;
            case BYTES_TYPE:
              buffer.putByteBuffer(column, getFile(cell)); // read filename
              break;
          }
        } catch (final IllegalArgumentException e) {
          throw new DbException(
              "Error parsing column "
                  + column
                  + " of row "
                  + lineNumber
                  + ", expected type: "
                  + schema.getColumnType(column)
                  + ", scanned value: "
                  + cell,
              e);
        }
      }
    }

    LOGGER.debug("Scanned {} input lines", lineNumber - lineNumberBegin);

    return buffer.popAny();
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public void close() throws IOException {
    parser = null;
    while (buffer.numTuples() > 0) {
      buffer.popAny();
    }
  }
  /**
   *
   */
  protected ByteBuffer getFile(final String filename) throws DbException {
    Preconditions.checkNotNull(filename, "byte[] filename was null");
    Path path = Paths.get(filename);
    byte[] data = null;
    try {
      data = Files.readAllBytes(path);
    } catch (IOException e) {
      throw new DbException(e);
    }
    return ByteBuffer.wrap(data);
  }
}
