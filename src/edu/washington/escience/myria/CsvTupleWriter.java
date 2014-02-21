package edu.washington.escience.myria;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;

import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import au.com.bytecode.opencsv.CSVWriter;

/**
 * CsvTupleWriter is a {@link TupleWriter} that serializes tuples to a delimited file, usually a CSV. It uses a
 * {@link CSVWriter} to do the underlying serialization. The fields to be output may contain special characters such as
 * newlines, because every field is quoted (using double quotes '"'). Double quotation marks inside of fields are
 * escaped using the CSV-standard trick of replacing '"' with '""'.
 * 
 * CSV files should be compatible with Microsoft Excel.
 * 
 * @author dhalperi
 * 
 */
public class CsvTupleWriter implements TupleWriter {

  /** The CSVWriter used to write the output. */
  private final CsvListWriter csvWriter;

  /**
   * Constructs a {@link CsvTupleWriter} object that will produce an Excel-compatible comma-separated value (CSV) file
   * from the provided tuples.
   * 
   * @param out the {@link OutputStream} to which the data will be written.
   */
  public CsvTupleWriter(final OutputStream out) {
    csvWriter = new CsvListWriter(new BufferedWriter(new OutputStreamWriter(out)), CsvPreference.STANDARD_PREFERENCE);
  }

  /**
   * Constructs a {@link CsvTupleWriter} object that will produce Excel-compatible comma-separated and tab-separated
   * files from the tuples in the provided queue.
   * 
   * @param separator the character used to separate fields in a line.
   * @param out the {@link OutputStream} to which the data will be written.
   */
  public CsvTupleWriter(final char separator, final OutputStream out) {
    final CsvPreference sepratorPreference =
        new CsvPreference.Builder(Character.toChars(CsvPreference.STANDARD_PREFERENCE.getQuoteChar())[0], separator,
            CsvPreference.STANDARD_PREFERENCE.getEndOfLineSymbols()).build();
    csvWriter = new CsvListWriter(new BufferedWriter(new OutputStreamWriter(out)), sepratorPreference);
  }

  @Override
  public void writeColumnHeaders(final List<String> columnNames) throws IOException {
    /* Begin by writing out the column names */
    final String[] row = new String[columnNames.size()];
    int headerCol = 0;
    for (String s : columnNames) {
      row[headerCol] = s;
      ++headerCol;
    }
    csvWriter.write(row);
  }

  @Override
  public void writeTuples(final ReadableTable tuples) throws IOException {
    final String[] row = new String[tuples.numColumns()];
    /* Serialize every row into the output stream. */
    for (int i = 0; i < tuples.numTuples(); ++i) {
      for (int j = 0; j < tuples.numColumns(); ++j) {
        row[j] = tuples.getObject(j, i).toString();
      }
      csvWriter.write(row);
    }
  }

  @Override
  public void done() throws IOException {
    csvWriter.flush();
    csvWriter.close();
  }
}
