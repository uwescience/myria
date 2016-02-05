package edu.washington.escience.myria;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.proto.Graph;

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
  //private transient CSVPrinter csvPrinter;
  private transient OutputStream outputStream;

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
    //csvPrinter = new CSVPrinter(new BufferedWriter(new OutputStreamWriter(out)), csvFormat);
    outputStream = out;
  }

  @Override
  public void writeColumnHeaders(final List<String> columnNames) throws IOException {
    //csvPrinter.printRecord(columnNames);
  }

  @Override
  public void writeTuples(final ReadableTable tuples) throws IOException {
    Graph.Vertices.Builder builder = Graph.Vertices.newBuilder();

    /* Serialize every row into the output stream. */
    for (int row = 0; row < tuples.numTuples(); ++row) {
      builder.addVertices(Graph.Vertex.newBuilder()
              .setId((int)tuples.getLong(0, row))
              .setValue(tuples.getDouble(1, row))
              .addEdge(Graph.Vertex.Edge.newBuilder()
                                   .setDestinationId((int)tuples.getLong(2, row))
                                   .setValue(tuples.getDouble(3, row)))
              .addEdge(Graph.Vertex.Edge.newBuilder()
                                   .setDestinationId((int)tuples.getLong(4, row))
                                   .setValue(tuples.getDouble(5, row)))
              .addEdge(Graph.Vertex.Edge.newBuilder()
                                   .setDestinationId((int)tuples.getLong(6, row))
                                   .setValue(tuples.getDouble(7, row))));
    }

    outputStream.write(builder.build().toByteArray());
  }

  @Override
  public void done() throws IOException {
    //csvPrinter.flush();
    //csvPrinter.close();
  }

  @Override
  public void error() throws IOException {
    try {
      //csvPrinter.print("There was an error. Investigate the query status to see the message");
    } finally {
      //csvPrinter.close();
    }
  }
}
