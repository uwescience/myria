package edu.washington.escience.myria;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringEscapeUtils;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.storage.ReadableTable;
import edu.washington.escience.myria.util.DateTimeUtils;

/**
 * JsonTupleWriter is a {@link TupleWriter} that serializes tuples to JavaScript Object Notation (JSON). The output is a
 * list of objects, each of which has one field 'attribute' : 'value' per column.
 *
 * For a dataset that has two integer columns names 'x' and 'y', and three rows, the output will look like
 *
 * <pre>
 * [{"x":1,"y":2},{"x":3,"y":4},{"x":5,"y":6}]
 * </pre>
 *
 * Attribute names (column names) are {@link String} objects that are escaped for JSON. For values, primitive types are
 * output unquoted and as-is; {@link DateTime} objects are quoted and serialized in ISO8601 format, and {@link String}
 * objects are quoted and escaped for JSON.
 *
 *
 */
public class JsonTupleWriter implements TupleWriter {

  /** Required for Java serialization. */
  static final long serialVersionUID = 1L;

  /** The names of the columns, escaped for JSON. */
  private ImmutableList<String> escapedColumnNames;
  /** The {@link PrintWriter} wraps the {@link OutputStream} to which we write the data. */
  private transient PrintWriter output;
  /** Whether we have output a single tuple yet. */
  private boolean haveWritten = false;

  /**
   * @param out the {@link OutputStream} to which the data will be written.
   * @throws IOException if there is an IO exception
   */
  @Override
  public void open(final OutputStream stream) throws IOException {
    output = new PrintWriter(new BufferedWriter(new OutputStreamWriter(stream)));
  }

  /**
   * @param ch the data to print
   * @throws IOException if the {@link PrintWriter} has errors.
   */
  private void print(final char ch) throws IOException {
    output.print(ch);
    if (output.checkError()) {
      throw new IOException("Broken pipe");
    }
  }

  /**
   * @param ch the data to print
   * @throws IOException if the {@link PrintWriter} has errors.
   */
  private void print(final boolean ch) throws IOException {
    output.print(ch);
    if (output.checkError()) {
      throw new IOException("Broken pipe");
    }
  }

  /**
   * @param ch the data to print
   * @throws IOException if the {@link PrintWriter} has errors.
   */
  private void print(final double ch) throws IOException {
    output.print(ch);
    if (output.checkError()) {
      throw new IOException("Broken pipe");
    }
  }

  /**
   * @param ch the data to print
   * @throws IOException if the {@link PrintWriter} has errors.
   */
  private void print(final int ch) throws IOException {
    output.print(ch);
    if (output.checkError()) {
      throw new IOException("Broken pipe");
    }
  }

  /**
   * @param ch the data to print
   * @throws IOException if the {@link PrintWriter} has errors.
   */
  private void print(final long ch) throws IOException {
    output.print(ch);
    if (output.checkError()) {
      throw new IOException("Broken pipe");
    }
  }

  /**
   * @param str the data to print
   * @throws IOException if the {@link PrintWriter} has errors.
   */
  private void print(final String str) throws IOException {
    output.print(str);
    if (output.checkError()) {
      throw new IOException("Broken pipe");
    }
  }

  @Override
  public void writeColumnHeaders(final List<String> columnNames) throws IOException {
    /* Generate and cache the escaped version of the column names. */
    Objects.requireNonNull(columnNames);
    ImmutableList.Builder<String> escapedNames = ImmutableList.builder();
    for (String name : columnNames) {
      escapedNames.add(StringEscapeUtils.escapeJson(name));
    }
    escapedColumnNames = escapedNames.build();

    /* Start the JSON with a '[' to open the list of objects. */
    print('[');
  }

  @Override
  public void writeTuples(final ReadableTable tuples) throws IOException {
    Objects.requireNonNull(escapedColumnNames);
    List<Type> columnTypes = tuples.getSchema().getColumnTypes();
    /* Add a record. */
    for (int i = 0; i < tuples.numTuples(); ++i) {
      /* Add the record separator (except first record) and open the record with '{'. */
      if (haveWritten) {
        print(",{");
      } else {
        haveWritten = true;
        print('{');
      }

      /*
       * Write each column of the record. Primitive types can be output as-is; {@link DateTime} objects are quoted and
       * serialized in ISO8601 format, and {@link String} objects are quoted and escaped for JSON.
       */
      for (int j = 0; j < tuples.numColumns(); ++j) {
        if (j > 0) {
          print(',');
        }
        print('"');
        print(escapedColumnNames.get(j));
        print("\":");
        switch (columnTypes.get(j)) {
          case BOOLEAN_TYPE:
            print(tuples.getBoolean(j, i));
            break;
          case DOUBLE_TYPE:
            print(tuples.getDouble(j, i));
            break;
          case FLOAT_TYPE:
            print(tuples.getFloat(j, i));
            break;
          case INT_TYPE:
            print(tuples.getInt(j, i));
            break;
          case LONG_TYPE:
            print(tuples.getLong(j, i));
            break;
          case DATETIME_TYPE:
            print('"');
            print(DateTimeUtils.dateTimeToISO8601(tuples.getDateTime(j, i)));
            print('"');
            break;
          case STRING_TYPE:
            print('"');
            print(StringEscapeUtils.escapeJson(tuples.getString(j, i)));
            print('"');
            break;
        }
      }
      print('}');
    }
  }

  @Override
  public void done() throws IOException {
    /* Close the list with ']'. */
    print(']');
    output.flush();
    output.close();
  }

  @Override
  public void error() throws IOException {
    try {
      if (haveWritten) {
        output.write(",{");
      } else {
        output.write("{");
      }
      output.write(
          "\"error\":\"There was an error. Investigate the query status to see the message\"}]");
      output.flush();
    } finally {
      output.close();
    }
  }
}
