package edu.washington.escience.myria;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;

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
  private CSVPrinter csvPrinter;

  /**
   * Constructs a {@link CsvTupleWriter} object that will produce an Excel-compatible comma-separated value (CSV) file
   * from the provided tuples.
   * 
   * @param out the {@link OutputStream} to which the data will be written.
   * @throws IOException if there is an IO exception
   */
  public CsvTupleWriter(final OutputStream out) throws IOException {
    this(out, CSVFormat.DEFAULT);
  }

  /**
   * Constructs a {@link CsvTupleWriter} object that will produce Excel-compatible comma-separated and tab-separated
   * files from the tuples in the provided queue.
   * 
   * @param separator the character used to separate fields in a line.
   * @param out the {@link OutputStream} to which the data will be written.
   * @throws IOException if there is an IO exception
   */
  public CsvTupleWriter(final char separator, final OutputStream out) throws IOException {
    this(out, CSVFormat.DEFAULT.withDelimiter(separator));
  }

  /**
   * @param out the {@link OutputStream} to which the data will be written.
   * @param csvFormat the CSV format.
   * @throws IOException if there is an IO exception
   */
  private CsvTupleWriter(final OutputStream out, final CSVFormat csvFormat) throws IOException {
    csvPrinter = new CSVPrinter(new BufferedWriter(new OutputStreamWriter(out)), csvFormat);
  }

  @Override
  public void open(final OutputStream stream) throws IOException {
    csvPrinter = new CSVPrinter(new BufferedWriter(new OutputStreamWriter(stream)), CSVFormat.DEFAULT);
  }

  @Override
  public void writeColumnHeaders(final List<String> columnNames) throws IOException {
    csvPrinter.printRecord(columnNames);
  }

  @Override
  public void writeTuples(final ReadableTable tuples) throws IOException {
    final String[] row = new String[tuples.numColumns()];
    /* Serialize every row into the output stream. */
    for (int i = 0; i < tuples.numTuples(); ++i) {
      for (int j = 0; j < tuples.numColumns(); ++j) {
        row[j] = tuples.getObject(j, i).toString();
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
}
