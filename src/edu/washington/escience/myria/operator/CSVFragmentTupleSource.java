/**
 *
 */
package edu.washington.escience.myria.operator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
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
public class CSVFragmentTupleSource extends LeafOperator {

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
  private long byteOverlap = MyriaConstants.PARALLEL_INGEST_BYTE_OVERLAP;
  private static final String truncatedQuoteErrorMessage =
      "EOF reached before encapsulated token finished";

  private boolean isLastWorker;
  private final long maxByteRange;
  private long partitionStartByteRange;
  private long partitionEndByteRange;

  private long adjustedStartByteRange;
  private int byteOffsetFromTruncatedRowAtStart = 0;
  private InputStream partitionInputStream;
  private CSVRecord record;
  private boolean onLastRow;
  private boolean finishedReadingLastRow;
  private boolean flagAsIncomplete;
  private boolean flagAsRangeSelected;
  private int[] workerIds;

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;

  /**
   * The logger for debug, trace, etc. messages in this class.
   */
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(CSVFragmentTupleSource.class);

  public CSVFragmentTupleSource(
      final String filename,
      final Schema schema,
      final long startByteRange,
      final long endByteRange,
      final boolean isLastWorker) {
    this(filename, schema, startByteRange, endByteRange, isLastWorker, null, null, null, null);
  }

  public CSVFragmentTupleSource(
      final DataSource source,
      final Schema schema,
      final long startByteRange,
      final long endByteRange,
      final boolean isLastWorker) {
    this(source, schema, startByteRange, endByteRange, isLastWorker, null, null, null, null);
  }

  public CSVFragmentTupleSource(
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

  public CSVFragmentTupleSource(
      final DataSource source,
      final Schema schema,
      final long startByteRange,
      final long endByteRange,
      final boolean isLastWorker,
      final Character delimiter) {
    this(source, schema, startByteRange, endByteRange, isLastWorker, delimiter, null, null, null);
  }

  public CSVFragmentTupleSource(
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

  public CSVFragmentTupleSource(
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
    flagAsIncomplete = false;
    flagAsRangeSelected = true;
  }

  public CSVFragmentTupleSource(
      final AmazonS3Source source,
      final Schema schema,
      final int[] workerIds,
      @Nullable final Character delimiter,
      @Nullable final Character quote,
      @Nullable final Character escape,
      @Nullable final Integer numberOfSkippedLines) {

    this.source = Preconditions.checkNotNull(source, "source");
    this.schema = Preconditions.checkNotNull(schema, "schema");
    this.workerIds = workerIds;

    this.delimiter = MoreObjects.firstNonNull(delimiter, CSVFormat.DEFAULT.getDelimiter());
    this.quote = MoreObjects.firstNonNull(quote, CSVFormat.DEFAULT.getQuoteCharacter());
    this.escape = escape;
    this.numberOfSkippedLines = MoreObjects.firstNonNull(numberOfSkippedLines, 0);

    maxByteRange = source.getFileSize();
    onLastRow = false;
    finishedReadingLastRow = false;
    flagAsIncomplete = false;
    flagAsRangeSelected = false;
  }

  @Override
  protected TupleBatch fetchNextReady() throws IOException, DbException {
    long lineNumberBegin = lineNumber;
    boolean nextRecordTruncated = false;

    while ((buffer.numTuples() < buffer.getBatchSize()) && !flagAsIncomplete) {
      lineNumber++;
      if (parser.isClosed()) {
        break;
      }

      if (nextRecordTruncated) {
        onLastRow = true;
      }

      try {
        if (!onLastRow) {
          record = iterator.next();
        }
      } catch (Exception e) {
        /*
         * FIX ME: If we hit an exception for a malformed row (in case of quotes for example), we mark this as the last
         * row
         */
        if (e.getMessage() != null && e.getMessage().contains(truncatedQuoteErrorMessage)) {
          onLastRow = true;
        } else {
          throw e;
        }
      }

      try {
        if (!iterator.hasNext()) {
          onLastRow = true;
        }
      } catch (Exception e) {
        /*
         * FIX ME: If we hit an exception for a malformed row (in case of quotes for example), we mark
         * nextRecordTruncated as true
         */
        if (e.getMessage() != null && e.getMessage().contains(truncatedQuoteErrorMessage)) {
          nextRecordTruncated = true;
        } else {
          throw e;
        }
      }

      /*
       * Here, if we are on the last row, we make sure to read the entire row until we either hit a new line or until we
       * have read the entire file (this is for the case where a single worker might be reading a single large row that
       * was split among other workers). If we're at the last row and the last worker is reading, we just mark this
       * final line as finished.
       */
      if (onLastRow && !finishedReadingLastRow && !isLastWorker) {
        long trailingStartByte = partitionEndByteRange + 1;
        long trailingEndByte = trailingStartByte + byteOverlap - 1;
        long finalBytePositionFound = trailingStartByte;
        boolean finalLineFound = false;

        while (!finalLineFound) {
          /*
           * If we are within the max byte range, then keep checking for a new line. Otherwise, if we've reached the end
           * of the file, mark finalLineFound as true.
           */
          if (trailingEndByte < maxByteRange) {
            InputStream trailingEndInputStream =
                source.getInputStream(trailingStartByte, trailingEndByte);
            int dataChar = trailingEndInputStream.read();
            while (dataChar != -1) {
              char currentChar = (char) dataChar;
              if (currentChar == '\n' || currentChar == '\r') {
                finalLineFound = true;
                break;
              }
              dataChar = trailingEndInputStream.read();
              finalBytePositionFound++;
            }
            trailingEndInputStream.close();
          } else {
            finalLineFound = true;
            finalBytePositionFound = maxByteRange;
          }

          /*
           * If we found the new line, then reset the parser for this line. Otherwise, increase the byte overlap and the
           * trailing range.
           */
          if (finalLineFound) {
            long characterPositionAtBeginningOfRecord =
                (record == null) ? 0 : record.getCharacterPosition();
            InputStream completePartitionStream =
                source.getInputStream(
                    adjustedStartByteRange + byteOffsetFromTruncatedRowAtStart,
                    finalBytePositionFound);
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
            if (nextRecordTruncated) {
              record = iterator.next();
            }
            finishedReadingLastRow = true;
          } else {
            trailingStartByte += byteOverlap;
            byteOverlap *= 2;
            trailingEndByte += byteOverlap;
          }
        }
      } else if (record.size() == schema.numColumns() && onLastRow && isLastWorker) {
        finishedReadingLastRow = true;
      }

      /*
       * If we're on the last row, we check if we've finished reading the row completely.
       */
      if (!onLastRow || (onLastRow && finishedReadingLastRow)) {
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
              case BLOB_TYPE:
                throw new DbException(
                    "Ingesting BLOB via csv isn't supported. Use DownloadBlob expression.");
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

    if (!flagAsRangeSelected) {
      int workerID = getNodeID();
      long fileSize = source.getFileSize();
      long currentPartitionSize = fileSize / workerIds.length;
      int workerIndex = -1;
      for (int i = 0; i < workerIds.length; i++) {
        if (workerID == workerIds[i]) {
          workerIndex = i;
        }
      }
      if (workerIndex >= 0) {
        boolean isLastWorker = workerIndex == workerIds.length - 1;
        long startByteRange = currentPartitionSize * workerIndex;
        long endByteRange;

        if (isLastWorker) {
          endByteRange = fileSize - 1;
        } else {
          endByteRange = (currentPartitionSize * (workerIndex + 1)) - 1;
        }
        this.partitionStartByteRange = startByteRange;
        this.partitionEndByteRange = endByteRange;
        this.isLastWorker = isLastWorker;
      } else {
        flagAsIncomplete = true;
      }
    }

    try {

      adjustedStartByteRange = partitionStartByteRange;
      /* Optimization */
      if (partitionStartByteRange != 0) {
        adjustedStartByteRange -= 1;
      }
      partitionInputStream = source.getInputStream(adjustedStartByteRange, partitionEndByteRange);

      /* If the file is empty, mark the partition as incomplete */
      if (maxByteRange == 0) {
        flagAsIncomplete = true;
      }

      /*
       * If this is not the first worker, we make sure to read until we hit a new line character. We do this to skip
       * partial rows at the beginning of the partition.
       */
      if (partitionStartByteRange != 0) {
        int firstChar = partitionInputStream.read();
        byteOffsetFromTruncatedRowAtStart = 1;
        if (firstChar != '\n' && firstChar != '\r') {
          boolean newLineFound = false;
          while (!newLineFound) {
            int currentChar = partitionInputStream.read();
            byteOffsetFromTruncatedRowAtStart++;
            if (currentChar == '\n' || currentChar == '\r' || currentChar == -1) {
              newLineFound = true;
              /*
               * If we never reach a new line (this could happen for a partial row at the last worker), mark as
               * incomplete
               */
              if (currentChar == -1) {
                flagAsIncomplete = true;
              } else if (currentChar == '\r') {
                currentChar = partitionInputStream.read();
                byteOffsetFromTruncatedRowAtStart++;
                if (currentChar != '\n') {
                  byteOffsetFromTruncatedRowAtStart--;
                  partitionInputStream =
                      source.getInputStream(
                          adjustedStartByteRange + byteOffsetFromTruncatedRowAtStart,
                          partitionEndByteRange);
                }
              }
            }
          }
        } else if (firstChar == '\r') {
          int currentChar = partitionInputStream.read();
          byteOffsetFromTruncatedRowAtStart++;
          if (currentChar != '\n') {
            byteOffsetFromTruncatedRowAtStart--;
            partitionInputStream =
                source.getInputStream(
                    adjustedStartByteRange + byteOffsetFromTruncatedRowAtStart,
                    partitionEndByteRange);
          }
        }
      }

      /* If we hit the end of the partition then mark it as incomplete.*/
      if (adjustedStartByteRange + byteOffsetFromTruncatedRowAtStart - 1 == partitionEndByteRange) {
        flagAsIncomplete = true;
      }

      /* If the partition is incomplete, do not instantiate the parser */
      if (!flagAsIncomplete) {
        parser =
            new CSVParser(
                new BufferedReader(new InputStreamReader(partitionInputStream)),
                CSVFormat.newFormat(delimiter).withQuote(quote).withEscape(escape));
        iterator = parser.iterator();

        /* FIX ME: For now, we only support cases where all skipped lines are contained within the first partition. */
        if (partitionStartByteRange == 0) {
          for (int i = 0; i < numberOfSkippedLines; i++) {
            iterator.next();
          }
        }
      }
    } catch (IOException e) {
      throw new DbException(e);
    }
  }
}
