package edu.washington.escience.myria;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Objects;

import org.joda.time.DateTime;

import com.google.common.collect.ImmutableList;
import com.sun.jersey.json.impl.writer.JsonEncoder;

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
 * @author dhalperi
 * 
 */
public class JsonTupleWriter implements TupleWriter {

  /** The names of the columns, escaped for JSON. */
  private ImmutableList<String> escapedColumnNames;
  /** The {@link PrintWriter} wraps the {@link OutputStream} to which we write the data. */
  private final PrintWriter output;
  /** Whether we have output a single tuple yet. */
  private boolean haveWritten = false;

  /**
   * Constructs a {@link JsonTupleWriter}.
   * 
   * @param output the {@link OutputStream} to which the data will be written.
   */
  public JsonTupleWriter(final OutputStream output) {
    this.output = new PrintWriter(new BufferedWriter(new OutputStreamWriter(output)));
  }

  @Override
  public void writeColumnHeaders(final List<String> columnNames) throws IOException {
    /* Generate and cache the escaped version of the column names. */
    Objects.requireNonNull(columnNames);
    ImmutableList.Builder<String> escapedNames = ImmutableList.builder();
    for (String name : columnNames) {
      escapedNames.add(JsonEncoder.encode(name));
    }
    escapedColumnNames = escapedNames.build();

    /* Start the JSON with a '[' to open the list of objects. */
    output.print('[');
  }

  @Override
  public void writeTuples(final TupleBatch tuples) throws IOException {
    Objects.requireNonNull(escapedColumnNames);
    List<Type> columnTypes = tuples.getSchema().getColumnTypes();
    /* Add a record. */
    for (int i = 0; i < tuples.numTuples(); ++i) {
      /* Add the record separator (except first record) and open the record with '{'. */
      if (haveWritten) {
        output.print(",{");
      } else {
        haveWritten = true;
        output.print('{');
      }

      /*
       * Write each column of the record. Primitive types can be output as-is; {@link DateTime} objects are quoted and
       * serialized in ISO8601 format, and {@link String} objects are quoted and escaped for JSON.
       */
      for (int j = 0; j < tuples.numColumns(); ++j) {
        if (j > 0) {
          output.print(',');
        }
        output.print('"');
        output.print(escapedColumnNames.get(j));
        output.print("\":");
        switch (columnTypes.get(j)) {
          case BOOLEAN_TYPE:
            output.print(tuples.getBoolean(j, i));
            break;
          case DOUBLE_TYPE:
            output.print(tuples.getDouble(j, i));
            break;
          case FLOAT_TYPE:
            output.print(tuples.getFloat(j, i));
            break;
          case INT_TYPE:
            output.print(tuples.getInt(j, i));
            break;
          case LONG_TYPE:
            output.print(tuples.getLong(j, i));
            break;
          case DATETIME_TYPE:
            output.print('"');
            output.print(DateTimeUtils.dateTimeToISO8601(tuples.getDateTime(j, i)));
            output.print('"');
            break;
          case STRING_TYPE:
            output.print('"');
            output.print(JsonEncoder.encode(tuples.getString(j, i)));
            output.print('"');
            break;
        }
      }
      output.print('}');
    }
  }

  @Override
  public void done() throws IOException {
    /* Close the list with ']'. */
    output.print(']');
    output.flush();
    output.close();
  }
}