package edu.washington.escience.myria.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;

import edu.washington.escience.myria.DbException;

/**
 * Utilities for string manipulation.
 *
 *
 */
public final class ErrorUtils {
  /**
   * Utility classes cannot be constructed.
   */
  private ErrorUtils() {}

  /**
   * @param cause any throwable object
   * @return the stack trace, as a string.
   */
  public static String getStackTrace(final Throwable cause) {
    StringWriter stringWriter = new StringWriter();
    cause.printStackTrace(new PrintWriter(stringWriter));
    return stringWriter.toString();
  }

  /**
   * @param msgBuilder message builder
   * @param e exception
   * @param prefix space prefix for each line.
   * */
  private static void mergeSQLExceptionMsg(
      final StringBuilder msgBuilder, final SQLException e, final String prefix) {
    msgBuilder.append(prefix + "ErrorCode: ");
    msgBuilder.append(e.getErrorCode());
    msgBuilder.append(", SQLState: ");
    msgBuilder.append(e.getSQLState());
    msgBuilder.append(", Msg: ");
    String m = e.getMessage();
    if (m != null) {
      msgBuilder.append(prefix + m.replaceAll("\n", "\n" + prefix));
    }
    if (e.getNextException() != null) {
      msgBuilder.append("\n");
      mergeSQLExceptionMsg(msgBuilder, e.getNextException(), prefix + "  ");
    }
  }

  /**
   * @param e exception
   * @return a DbException with all useful error information encoded into the error message while keeping the e as the
   *         cause.
   * */
  public static DbException mergeSQLException(final SQLException e) {
    StringBuilder msgBuilder = new StringBuilder();
    mergeSQLExceptionMsg(msgBuilder, e, "");
    return new DbException(msgBuilder.toString(), e);
  }
}
