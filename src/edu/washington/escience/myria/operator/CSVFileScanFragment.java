/**
 *
 */
package edu.washington.escience.myria.operator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import javax.annotation.Nullable;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.BooleanUtils;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Floats;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.io.FileSource;
import edu.washington.escience.myria.io.UriSource;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.DateTimeUtils;

/**
 * 
 */
public class CSVFileScanFragment extends LeafOperator {

  /** The Schema of the relation stored in this file. */
  private final Schema schema;
  /** Scanner used to parse the file. */
  private transient CSVParser parser = null;
  /** Iterator over CSV records. */
  private transient Iterator<CSVRecord> iterator = null;
  /** A user-provided file delimiter; if null, the system uses the default comma as delimiter. */
  private final Character delimiter;
  /** A user-provided quotation mark, if null, the system uses '"'. */
  private final Character quote;
  /** A user-provided escape character to escape quote and itself, if null, the system uses '/'. */
  private final Character escape;
  /** The data source that will generate the input stream to be read at initialization. */
  private final UriSource source;
  /** Number of skipped lines on the head. */
  private final Integer numberOfSkippedLines;
  /** Holds the tuples that are ready for release. */
  private transient TupleBatchBuffer buffer;
  /** Which line of the file the scanner is currently on. */
  private long lineNumber = 0;

  private final int workerID;
  private final int totalWorkers;
  private final boolean lastWorker;

  private final long fileSize;
  private final long partitionSize;
  private final long OVERLAP = 1;
  long start;
  long end;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The logger for debug, trace, etc. messages in this class.
   */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(CSVFileScanFragment.class);

  public CSVFileScanFragment(final String filename, final Schema schema, final int workerID, final int totalWorkers,
      final boolean lastWorker) {
    this(filename, schema, workerID, totalWorkers, lastWorker, null, null, null, null);
  }

  public CSVFileScanFragment(final DataSource source, final Schema schema, final int workerID, final int totalWorkers,
      final boolean lastWorker) {
    this(source, schema, workerID, totalWorkers, lastWorker, null, null, null, null);
  }

  public CSVFileScanFragment(final String filename, final Schema schema, final int workerID, final int totalWorkers,
      final boolean lastWorker, final Character delimiter) {
    this(new FileSource(filename), schema, workerID, totalWorkers, lastWorker, delimiter, null, null, null);
  }

  public CSVFileScanFragment(final DataSource source, final Schema schema, final int workerID, final int totalWorkers,
      final boolean lastWorker, final Character delimiter) {
    this(source, schema, workerID, totalWorkers, lastWorker, delimiter, null, null, null);
  }

  public CSVFileScanFragment(final String filename, final Schema schema, final int workerID, final int totalWorkers,
      final boolean lastWorker, @Nullable final Character delimiter, @Nullable final Character quote,
      @Nullable final Character escape, @Nullable final Integer numberOfSkippedLines) {
    this(new FileSource(filename), schema, workerID, totalWorkers, lastWorker, delimiter, quote, escape,
        numberOfSkippedLines);
  }

  public CSVFileScanFragment(final DataSource source, final Schema schema, final int workerID, final int totalWorkers,
      final boolean lastWorker, @Nullable final Character delimiter, @Nullable final Character quote,
      @Nullable final Character escape, @Nullable final Integer numberOfSkippedLines) {
    this.source = (UriSource) Preconditions.checkNotNull(source, "source");
    this.schema = Preconditions.checkNotNull(schema, "schema");

    this.delimiter = MoreObjects.firstNonNull(delimiter, CSVFormat.DEFAULT.getDelimiter());
    this.quote = MoreObjects.firstNonNull(quote, CSVFormat.DEFAULT.getQuoteCharacter());
    this.escape = escape != null ? escape : CSVFormat.DEFAULT.getEscapeCharacter();
    this.numberOfSkippedLines = MoreObjects.firstNonNull(numberOfSkippedLines, 0);

    this.workerID = workerID;
    this.totalWorkers = totalWorkers;
    this.lastWorker = lastWorker;

    fileSize = ((UriSource) source).getFileSize();
    partitionSize = fileSize / totalWorkers;
    start = partitionSize * (workerID - 1);
    end = start + partitionSize;
  }

  @Override
  protected TupleBatch fetchNextReady() throws IOException, DbException {
    long lineNumberBegin = lineNumber;
    boolean first = true;

    while ((buffer.numTuples() < TupleBatch.BATCH_SIZE)) {
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
        if (lineNumber != 0 && !lastWorker) {
          long byteAtBeginning = record.getCharacterPosition();
          LOGGER.warn("BYTE " + byteAtBeginning);
          if (first) {
            first = false;
            start = byteAtBeginning;
          }
          end = end + OVERLAP;
          LOGGER.warn("NEW START " + start);
          LOGGER.warn("NEW END " + end);

          parser = new CSVParser(new BufferedReader(new InputStreamReader(source.getChunkInputStream(start, end,
              lastWorker))), CSVFormat.newFormat(delimiter).withQuote(quote).withEscape(escape));
          // FLAG
          iterator = parser.iterator();

        } else {
          LOGGER.warn("IGNORE THIS CASE ");
        }
      } else {
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
            }
          } catch (final IllegalArgumentException e) {
            throw new DbException("Error parsing column " + column + " of row " + lineNumber + ", expected type: "
                + schema.getColumnType(column) + ", scanned value: " + cell, e);
          }
        }
      }

    }

    LOGGER.debug("Scanned {} input lines", lineNumber - lineNumberBegin);

    return buffer.popAny();

  }

  @Override
  protected Schema generateSchema() {
    return schema;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    buffer = new TupleBatchBuffer(getSchema());
    try {
      parser = new CSVParser(new BufferedReader(new InputStreamReader(source.getChunkInputStream(start, end,
          lastWorker))), CSVFormat.newFormat(delimiter).withQuote(quote).withEscape(escape));
      iterator = parser.iterator();
      for (int i = 0; i < numberOfSkippedLines; i++) {
        iterator.next();
      }
    } catch (IOException e) {
      throw new DbException(e);
    }

    lineNumber = 0;
  }
}
