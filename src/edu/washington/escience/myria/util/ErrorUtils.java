package edu.washington.escience.myria.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Utilities for string manipulation.
 * 
 * @author dhalperi
 * 
 */
public final class ErrorUtils {
  /**
   * Utility classes cannot be constructed.
   */
  private ErrorUtils() {
  }

  /**
   * @param cause any throwable object
   * @return the stack trace, as a string.
   */
  public static String getStackTrace(final Throwable cause) {
    StringWriter stringWriter = new StringWriter();
    cause.printStackTrace(new PrintWriter(stringWriter));
    return stringWriter.toString();
  }
}