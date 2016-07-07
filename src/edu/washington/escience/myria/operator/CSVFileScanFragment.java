/**
 *
 */
package edu.washington.escience.myria.operator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
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
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.io.AmazonS3Source;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.io.FileSource;
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
  private final AmazonS3Source source;
  /** Number of skipped lines on the head. */
  private final Integer numberOfSkippedLines;
  /** Holds the tuples that are ready for release. */
  private transient TupleBatchBuffer buffer;
  /** Which line of the file the scanner is currently on. */
  private long lineNumber = 0;

  private final boolean isLastWorker;

  private long byteOverlap = MyriaConstants.BYTE_OVERLAP_PARALLEL_INGEST;
  private long partitionStartByteRange;
  private long partitionEndByteRange;

  private InputStream partitionInputStream;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The logger for debug, trace, etc. messages in this class.
   */
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(CSVFileScanFragment.class);

  public CSVFileScanFragment(final String filename, final Schema schema, final long startByteRange,
      final long endByteRange, final boolean isLastWorker) {
    this(filename, schema, startByteRange, endByteRange, isLastWorker, null, null, null, null);
  }

  public CSVFileScanFragment(final DataSource source, final Schema schema, final long startByteRange,
      final long endByteRange, final boolean isLastWorker) {
    this(source, schema, startByteRange, endByteRange, isLastWorker, null, null, null, null);
  }

  public CSVFileScanFragment(final String filename, final Schema schema, final long startByteRange,
      final long endByteRange, final boolean isLastWorker, final Character delimiter) {
    this(new FileSource(filename), schema, startByteRange, endByteRange, isLastWorker, delimiter, null, null, null);
  }

  public CSVFileScanFragment(final DataSource source, final Schema schema, final long startByteRange,
      final long endByteRange, final boolean isLastWorker, final Character delimiter) {
    this(source, schema, startByteRange, endByteRange, isLastWorker, delimiter, null, null, null);
  }

  public CSVFileScanFragment(final String filename, final Schema schema, final long startByteRange,
      final long endByteRange, final boolean isLastWorker, @Nullable final Character delimiter,
      @Nullable final Character quote, @Nullable final Character escape, @Nullable final Integer numberOfSkippedLines) {
    this(new FileSource(filename), schema, startByteRange, endByteRange, isLastWorker, delimiter, quote, escape,
        numberOfSkippedLines);
  }

  public CSVFileScanFragment(final DataSource source, final Schema schema, final long partitionStartByteRange,
      final long partitionEndByteRange, final boolean isLastWorker, @Nullable final Character delimiter,
      @Nullable final Character quote, @Nullable final Character escape, @Nullable final Integer numberOfSkippedLines) {
    this.source = (AmazonS3Source) Preconditions.checkNotNull(source, "source");
    this.schema = Preconditions.checkNotNull(schema, "schema");

    this.delimiter = MoreObjects.firstNonNull(delimiter, CSVFormat.DEFAULT.getDelimiter());
    this.quote = MoreObjects.firstNonNull(quote, CSVFormat.DEFAULT.getQuoteCharacter());
    this.escape = escape != null ? escape : CSVFormat.DEFAULT.getEscapeCharacter();
    this.numberOfSkippedLines = MoreObjects.firstNonNull(numberOfSkippedLines, 0);

    this.partitionStartByteRange = partitionStartByteRange;
    this.partitionEndByteRange = partitionEndByteRange;
    this.isLastWorker = isLastWorker;

  }

  @Override
  protected TupleBatch fetchNextReady() throws IOException, DbException {
    long lineNumberBegin = lineNumber;
    boolean fixingStartByte = false;
    boolean onLastRow = false;

    boolean discardedRecord = false;

    while ((buffer.numTuples() < TupleBatch.BATCH_SIZE)) {
      discardedRecord = false;
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
      // This covers the case where the first row of a worker matches the schema. We only want to read this row if the
      // previous character is '\n' or '\r'
      if (record.size() == schema.numColumns() && lineNumber - 1 == 0 && partitionStartByteRange != 0) {
        InputStreamReader startStreamReader = new InputStreamReader(partitionInputStream);
        char currentChar = (char) startStreamReader.read();
        if (currentChar != '\n' && currentChar != '\r') {
          discardedRecord = true;
        }
      }
      // This covers the case where the last row matches the schema's number of columns. We need to ensure that we read
      // the entire row completely
      else if (record.size() == schema.numColumns() && !iterator.hasNext() && !isLastWorker) {
        long movingEndByte = partitionEndByteRange;
        long bytePositionAtBeginningOfRecord = record.getCharacterPosition();
        boolean newLineFound = false;
        while (!newLineFound) {
          movingEndByte += MyriaConstants.BYTE_OVERLAP_PARALLEL_INGEST;
          // Create a stream to look for the new line
          InputStream trailingEndInputStream = source.getInputStream(partitionEndByteRange, movingEndByte);
          InputStreamReader startStreamReader = new InputStreamReader(trailingEndInputStream);
          int dataChar = startStreamReader.read();
          while (dataChar != -1) {
            char currentChar = (char) dataChar;
            if (currentChar == '\n' || currentChar == '\r') {
              newLineFound = true;
              // Re-initialize the parser with the last row only
              InputStream beginningOfRecord =
                  source.getInputStream(bytePositionAtBeginningOfRecord, partitionEndByteRange);
              InputStream concatenateEndOfRecord = source.getInputStream(partitionEndByteRange + 1, movingEndByte);
              partitionInputStream = new SequenceInputStream(beginningOfRecord, concatenateEndOfRecord);
              parser =
                  new CSVParser(new BufferedReader(new InputStreamReader(partitionInputStream)), CSVFormat.newFormat(
                      delimiter).withQuote(quote).withEscape(escape), bytePositionAtBeginningOfRecord, 0);
              iterator = parser.iterator();
              onLastRow = true;
              record = iterator.next();
              break;
            }
            dataChar = startStreamReader.read();
          }
          startStreamReader.close();
        }
      }
      // This is a partial fragment, columns are missing and we need to read more bytes
      if (record.size() < schema.numColumns()) {
        if (lineNumber - 1 != 0 && !isLastWorker) {
          onLastRow = true;
          long bytePositionAtBeginningOfRecord = record.getCharacterPosition();
          if (!fixingStartByte) {
            fixingStartByte = true;
            partitionStartByteRange += bytePositionAtBeginningOfRecord;
          }
          partitionEndByteRange += byteOverlap;
          InputStream overlapStream = source.getInputStream(partitionStartByteRange, partitionEndByteRange);
          partitionInputStream = new SequenceInputStream(partitionInputStream, overlapStream);
          parser =
              new CSVParser(new BufferedReader(new InputStreamReader(partitionInputStream)), CSVFormat.newFormat(
                  delimiter).withQuote(quote).withEscape(escape));
          iterator = parser.iterator();
          byteOverlap *= 2;
        } else {
          discardedRecord = true;
        }
      } else if (!discardedRecord) {
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
          if (onLastRow) {
            parser.close();

          }
        }
      }

    }

    LOGGER.debug("Scanned {} input lines", lineNumber - lineNumberBegin);

    return buffer.popAny();

  }

  @Override
  public void cleanup() throws IOException {
    parser = null;
    while (buffer.numTuples() > 0) {
      buffer.popAny();
    }
  }

  @Override
  protected Schema generateSchema() {
    return schema;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    buffer = new TupleBatchBuffer(getSchema());
    try {
      // (Optimization) Read the preceding byte if this is not the first worker
      if (partitionStartByteRange != 0) {
        partitionStartByteRange = partitionStartByteRange - 1;
      }
      partitionInputStream = source.getInputStream(partitionStartByteRange, partitionEndByteRange);
      parser =
          new CSVParser(new BufferedReader(new InputStreamReader(partitionInputStream)), CSVFormat.newFormat(delimiter)
              .withQuote(quote).withEscape(escape));
      iterator = parser.iterator();
      for (int i = 0; i < numberOfSkippedLines; i++) {
        iterator.next();
      }
    } catch (IOException e) {
      throw new DbException(e);
    }
  }
}