package edu.washington.escience.myria;

import java.io.BufferedWriter;
import java.io.File;
import java.nio.file.Files;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import edu.washington.escience.myria.storage.ReadableTable;

/**
 * CsvTupleWriter is a {@link TupleWriter} that serializes tuples to a delimited file, usually a CSV. It uses a
 * {@link CSVPrinter} to do the underlying serialization. The fields to be output may contain special characters such as
 * newlines, because fields may be quoted (using double quotes '"'). Double quotation marks inside of fields are escaped
 * using the CSV-standard trick of replacing '"' with '""'.
 *
 * CSV files should be compatible with Microsoft Excel.
 *
 */
public class CsvTupleWriter implements TupleWriter {

  /** Required for Java serialization. */
  static final long serialVersionUID = 1L;

  /** The CSVWriter used to write the output. */
  private transient CSVPrinter csvPrinter;

  /** The CSV format **/
  final CSVFormat csvFormat;

  public CsvTupleWriter() {
    this(CSVFormat.DEFAULT);
  }

  public CsvTupleWriter(final char separator) {
    this(CSVFormat.DEFAULT.withDelimiter(separator));
  }

  public CsvTupleWriter(final CSVFormat format) {
    csvFormat = format;
  }

  @Override
  public void open(final OutputStream out) throws IOException {
    csvPrinter = new CSVPrinter(new BufferedWriter(new OutputStreamWriter(out)), csvFormat);
  }

  @Override
  public void writeColumnHeaders(final List<String> columnNames) throws IOException {
    csvPrinter.printRecord(columnNames);
  }

  @Override
  public void writeTuples(final ReadableTable tuples) throws IOException {
    final String[] row = new String[tuples.numColumns()];
    /* Serialize every row into the output stream. */
    Schema tbsc = tuples.getSchema();
    for (int i = 0; i < tuples.numTuples(); ++i) {
      for (int j = 0; j < tuples.numColumns(); ++j) {
        Type type = tbsc.getColumnType(j);
        if (type.equals(Type.BLOB_TYPE)) {
          // write the file out
          ByteBuffer bb = tuples.getBlob(j, i);
          String filename = createTempFile(bb);
          //add file name to the csv.
          row[j] = filename;
        } else {
          row[j] = tuples.getObject(j, i).toString();
        }
      }
      csvPrinter.printRecord((Object[]) row);
    }
  }

  @Override
  public void done() throws IOException {
    csvPrinter.flush();
    csvPrinter.close();
  }

  @Override
  public void error() throws IOException {
    try {
      csvPrinter.print("There was an error. Investigate the query status to see the message");
    } finally {
      csvPrinter.close();
    }
  }

  private static String createTempFile(final ByteBuffer bb) throws IOException {
    Path path = Files.createTempFile("out", null);
    File file = path.toFile();
    Files.write(path, bb.array());
    return file.getAbsolutePath();
  }
}
