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
  private long byteOverlap = MyriaConstants.BYTE_OVERLAP_PARALLEL_INGEST;

  private final boolean isLastWorker;
  private final long maxByteRange;
  private final long partitionStartByteRange;
  private final long partitionEndByteRange;

  private long adjustedStartByteRange;
  private long adjustedEndByteRange;
  private InputStream partitionInputStream;
  private CSVRecord record;
  private boolean onLastRow;
  private boolean finishedReadingLastRow;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The logger for debug, trace, etc. messages in this class.
   */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(CSVFileScanFragment.class);

  public CSVFileScanFragment(
      final String filename,
      final Schema schema,
      final long startByteRange,
      final long endByteRange,
      final boolean isLastWorker) {
    this(filename, schema, startByteRange, endByteRange, isLastWorker, null, null, null, null);
  }

  public CSVFileScanFragment(
      final DataSource source,
      final Schema schema,
      final long startByteRange,
      final long endByteRange,
      final boolean isLastWorker) {
    this(source, schema, startByteRange, endByteRange, isLastWorker, null, null, null, null);
  }

  public CSVFileScanFragment(
      final String filename,
      final Schema schema,
      final long startByteRange,
      final long endByteRange,
      final boolean isLastWorker,
      final Character delimiter) {
    this(
        new FileSource(filename),
        schema,
        startByteRange,
        endByteRange,
        isLastWorker,
        delimiter,
        null,
        null,
        null);
  }

  public CSVFileScanFragment(
      final DataSource source,
      final Schema schema,
      final long startByteRange,
      final long endByteRange,
      final boolean isLastWorker,
      final Character delimiter) {
    this(source, schema, startByteRange, endByteRange, isLastWorker, delimiter, null, null, null);
  }

  public CSVFileScanFragment(
      final String filename,
      final Schema schema,
      final long startByteRange,
      final long endByteRange,
      final boolean isLastWorker,
      @Nullable final Character delimiter,
      @Nullable final Character quote,
      @Nullable final Character escape,
      @Nullable final Integer numberOfSkippedLines) {
    this(
        new FileSource(filename),
        schema,
        startByteRange,
        endByteRange,
        isLastWorker,
        delimiter,
        quote,
        escape,
        numberOfSkippedLines);
  }

  public CSVFileScanFragment(
      final DataSource source,
      final Schema schema,
      final long partitionStartByteRange,
      final long partitionEndByteRange,
      final boolean isLastWorker,
      @Nullable final Character delimiter,
      @Nullable final Character quote,
      @Nullable final Character escape,
      @Nullable final Integer numberOfSkippedLines) {
    this.source = (AmazonS3Source) Preconditions.checkNotNull(source, "source");
    this.schema = Preconditions.checkNotNull(schema, "schema");

    this.delimiter = MoreObjects.firstNonNull(delimiter, CSVFormat.DEFAULT.getDelimiter());
    this.quote = MoreObjects.firstNonNull(quote, CSVFormat.DEFAULT.getQuoteCharacter());
    this.escape = escape;
    this.numberOfSkippedLines = MoreObjects.firstNonNull(numberOfSkippedLines, 0);

    this.partitionStartByteRange = partitionStartByteRange;
    this.partitionEndByteRange = partitionEndByteRange;
    this.isLastWorker = isLastWorker;

    maxByteRange = ((AmazonS3Source) source).getFileSize();
    onLastRow = false;
    finishedReadingLastRow = false;
  }

  @Override
  protected TupleBatch fetchNextReady() throws IOException, DbException {
    long lineNumberBegin = lineNumber;
    boolean discardedRecord = false;

    while ((buffer.numTuples() < TupleBatch.BATCH_SIZE)) {
      discardedRecord = false;
      lineNumber++;
      if (parser.isClosed()) {
        break;
      }

      if (!onLastRow) {
        record = iterator.next();
      }

      try {
        if (!iterator.hasNext()) {
          onLastRow = true;
        }
      } catch (Exception e) {
      }

      if (record != null) {
        /*
         * Here, we read the first byte of the stream if this is not the first worker. This is simply an optimization.
         * If the previous character is not a new line, we discard the line.
         */
        if (lineNumber - 1 == 0 && partitionStartByteRange != 0) {
          char currentChar = (char) partitionInputStream.read();
          if (currentChar != '\n' && currentChar != '\r') {
            discardedRecord = true;
            /*
             * This handles closes the stream in corner cases in which a worker might only have a fragment of a tuple as
             * its entire partition. Since the server makes sure to provide workers a partition with a minimum size,
             * this case could only happen with very large rows.
             */
            if (onLastRow) {
              parser.close();
              break;
            }
          } else {
            /*
             * This handles the case in which the splitting occurs in the middle of \r\n.
             */
            currentChar = (char) partitionInputStream.read();
            if (currentChar == '\n') {
              discardedRecord = true;
            }
          }
        }

        /*
         * Here, if we are on the last row, we make sure to read the entire row until we either hit a new line or until
         * we have read the entire file (again, this is for the case where a single worker might be reading a single
         * large row that was split among other workers)
         */
        if (onLastRow && !finishedReadingLastRow && !isLastWorker) {
          long movingEndByte = adjustedEndByteRange + 1;
          long characterPositionAtBeginningOfRecord = record.getCharacterPosition();
          boolean newLineFound = false;
          while (!newLineFound) {
            // In order to find the new line character, we use SequenceInputStream to pipe in more bytes
            InputStream trailingEndInputStream =
                source.getInputStream(
                    movingEndByte, Math.min(movingEndByte + byteOverlap, maxByteRange));
            movingEndByte += byteOverlap;
            partitionInputStream =
                new SequenceInputStream(partitionInputStream, trailingEndInputStream);
            int dataChar = partitionInputStream.read();
            while (dataChar != -1) {
              char currentChar = (char) dataChar;
              if (currentChar == '\n'
                  || currentChar == '\r'
                  || Math.min(movingEndByte, maxByteRange) == maxByteRange) {
                newLineFound = true;
                // Reading from the source from the beginning again since we only have the character offset information
                InputStream completePartitionStream =
                    source.getInputStream(
                        adjustedStartByteRange, Math.min(movingEndByte, maxByteRange));
                BufferedReader reader =
                    new BufferedReader(new InputStreamReader(completePartitionStream));
                reader.skip(characterPositionAtBeginningOfRecord);
                parser =
                    new CSVParser(
                        reader,
                        CSVFormat.newFormat(delimiter).withQuote(quote).withEscape(escape),
                        0,
                        0);
                iterator = parser.iterator();
                record = iterator.next();
                finishedReadingLastRow = true;
                break;
              }
              dataChar = partitionInputStream.read();
            }
            partitionInputStream.close();
            byteOverlap *= 2;
          }
        } else if (record.size() == schema.numColumns() && onLastRow && isLastWorker) {
          /*
           * If we are on the last worker, just mark it as finished.
           */
          finishedReadingLastRow = true;
        }

        /*
         * At this point, we've exhausted all other cases and check for two conditions. 1) The record is not discarded
         * and 2) If the we are on the last row, we need to make sure we've finished reading the entire row
         */
        if (((!onLastRow) || (onLastRow && finishedReadingLastRow)) && !discardedRecord) {
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
          /*
           * Once we finish reading the last row, we close the parser
           */
          if (onLastRow) {
            parser.close();
          }
        }
        LOGGER.debug("Scanned {} input lines", lineNumber - lineNumberBegin);
      }
    }
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
      /* Optimization : if not the first worker, read the previous byte */
      adjustedStartByteRange = partitionStartByteRange;
      if (partitionStartByteRange != 0) {
        adjustedStartByteRange -= 1;
      }
      adjustedEndByteRange = partitionEndByteRange;
      partitionInputStream = source.getInputStream(adjustedStartByteRange, adjustedEndByteRange);
      parser =
          new CSVParser(
              new BufferedReader(new InputStreamReader(partitionInputStream)),
              CSVFormat.newFormat(delimiter).withQuote(quote).withEscape(escape));
      iterator = parser.iterator();
      for (int i = 0; i < numberOfSkippedLines; i++) {
        iterator.next();
      }
    } catch (IOException e) {
      throw new DbException(e);
    }
  }
}
