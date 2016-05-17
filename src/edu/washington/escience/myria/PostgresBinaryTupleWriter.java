package edu.washington.escience.myria;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

import edu.washington.escience.myria.storage.ReadableTable;

/**
 * PostgresBinaryTupleWriter is a {@link TupleWriter} that serializes tuples to a a binary format that can be directly
 * imported into PostgreSQL. See http://www.postgresql.org/docs/current/interactive/sql-copy.html.
 *
 * This requires integer time stamps.
 */
public class PostgresBinaryTupleWriter implements TupleWriter {

  /** Required for Java serialization. */
  static final long serialVersionUID = 1L;

  /** The ByteBuffer to write the output. */
  private transient DataOutputStream buffer;

  /**
   * @param out the {@link OutputStream} to which the data will be written.
   * @throws IOException if there is an IO exception
   */
  @Override
  public void open(final OutputStream out) throws IOException {
    buffer = new DataOutputStream(new BufferedOutputStream(out));
    // 11 bytes required header
    buffer.writeBytes("PGCOPY\n\377\r\n\0");
    // 32 bit integer indicating no OID
    buffer.writeInt(0);
    // 32 bit header extension area length
    buffer.writeInt(0);
  }

  /*
   * No-op
   */
  @Override
  public void writeColumnHeaders(final List<String> columnNames) throws IOException {}

  /**
   * Converts the given java seconds to postgresql seconds. The conversion is valid for any year 100 BC onwards.
   *
   * from /org/postgresql/jdbc2/TimestampUtils.java
   *
   * @param seconds Postgresql seconds.
   * @return Java seconds.
   */
  @SuppressWarnings("checkstyle:magicnumber")
  private static long toPgSecs(final long seconds) {
    long secs = seconds;
    // java epoc to postgres epoc
    secs -= 946684800L;

    // Julian/Greagorian calendar cutoff point
    if (secs < -13165977600L) { // October 15, 1582 -> October 4, 1582
      secs -= 86400 * 10;
      if (secs < -15773356800L) { // 1500-03-01 -> 1500-02-28
        int years = (int) ((secs + 15773356800L) / -3155823050L);
        years++;
        years -= years / 4;
        secs += years * 86400;
      }
    }

    return secs;
  }

  @Override
  public void writeTuples(final ReadableTable tuples) throws IOException {
    List<Type> columnTypes = tuples.getSchema().getColumnTypes();

    for (int i = 0; i < tuples.numTuples(); ++i) {
      short numColumns = (short) tuples.numColumns();

      // 16 bit integer number of tuples
      buffer.writeShort(numColumns);
      for (int j = 0; j < numColumns; ++j) {

        // 32 bit integer for length of value
        // n bytes value

        switch (columnTypes.get(j)) {
          case BOOLEAN_TYPE:
            // https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/bool.c

            buffer.writeInt(1);
            if (tuples.getBoolean(j, i)) {
              buffer.writeByte(1);
            } else {
              buffer.writeByte(0);
            }
            break;
          case DOUBLE_TYPE:
            // https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/float.c

            buffer.writeInt(8);
            buffer.writeDouble(tuples.getDouble(j, i));
            break;
          case FLOAT_TYPE:
            // https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/float.c

            buffer.writeInt(4);
            buffer.writeFloat(tuples.getFloat(j, i));
            break;
          case INT_TYPE:
            // https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/int.c

            buffer.writeInt(4);
            buffer.writeInt(tuples.getInt(j, i));
            break;
          case LONG_TYPE:
            // https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/int8.c

            buffer.writeInt(8);
            buffer.writeLong(tuples.getLong(j, i));
            break;
          case DATETIME_TYPE:
            // https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/timestamp.c

            // requires eight-byte integers for time stamps! This should be the default.
            // See http://www.postgresql.org/docs/9.1/static/datatype-datetime.html

            buffer.writeInt(8);

            DateTime theTime = tuples.getDateTime(j, i);
            long millis = theTime.getMillis();

            // adjust time zone offset
            millis += theTime.getZone().getOffset(millis);

            // pg time 0 is 2000-01-01 00:00:00
            long secs = toPgSecs(TimeUnit.MILLISECONDS.toSeconds(millis));

            buffer.writeLong(TimeUnit.SECONDS.toMicros(secs));
            break;
          case STRING_TYPE:
            // https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/varchar.c

            String string = tuples.getString(j, i);
            final byte[] utf8Bytes = string.getBytes("UTF-8");
            buffer.writeInt(utf8Bytes.length);
            buffer.write(utf8Bytes);
            break;
        }
      }
    }

    // 16 bit file trailer
    buffer.writeShort(-1);
  }

  @Override
  public void done() throws IOException {
    buffer.flush();
    buffer.close();
  }

  @Override
  public void error() throws IOException {
    try {
      throw new IOException("An error ocurred when writing binary data.");
    } finally {
      buffer.close();
    }
  }
}
