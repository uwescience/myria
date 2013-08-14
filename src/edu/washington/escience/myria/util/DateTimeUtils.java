package edu.washington.escience.myria.util;

import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Utility functions about date time.
 * */
public final class DateTimeUtils {

  /**
   * util class.
   * */
  private DateTimeUtils() {
  }

  /**
   * If time elapse is more than a day, use this format.
   * */
  public static final String DAY_ELAPSE_FORMAT = "%1$dd %2$dh %3$dm %4$d.%5$03ds";
  /**
   * If time elapse is less than a day, but more than an hour, use this format.
   * */
  public static final String HOUR_ELAPSE_FORMAT = "%2$dh %3$dm %4$d.%5$03ds";
  /**
   * If time elapse is less than an hour, but more than a minute, use this format.
   * */
  public static final String MINUTE_ELAPSE_FORMAT = "%3$dm %4$d.%5$03ds";
  /**
   * If time elapse is less than a minute use this format.
   * */
  public static final String SECOND_ELAPSE_FORMAT = "%4$d.%5$03ds";

  /**
   * @return Convert nanoElapse to human readable format.
   * @param nanoElapse time elapse in nano seconds.
   * */
  public static String nanoElapseToHumanReadable(final long nanoElapse) {
    long nanoElapseLocal = nanoElapse;
    String elapseFormat = null;
    final long day = TimeUnit.NANOSECONDS.toDays(nanoElapseLocal);
    nanoElapseLocal -= TimeUnit.DAYS.toNanos(day);
    if (day > 0) {
      elapseFormat = DAY_ELAPSE_FORMAT;
    }
    final long hour = TimeUnit.NANOSECONDS.toHours(nanoElapseLocal);
    nanoElapseLocal -= TimeUnit.HOURS.toNanos(hour);
    if (hour > 0 && elapseFormat == null) {
      elapseFormat = HOUR_ELAPSE_FORMAT;
    }
    final long minute = TimeUnit.NANOSECONDS.toMinutes(nanoElapseLocal);
    nanoElapseLocal -= TimeUnit.MINUTES.toNanos(minute);
    if (minute > 0 && elapseFormat == null) {
      elapseFormat = MINUTE_ELAPSE_FORMAT;
    }
    final long second = TimeUnit.NANOSECONDS.toSeconds(nanoElapseLocal);
    nanoElapseLocal -= TimeUnit.SECONDS.toNanos(second);
    if (elapseFormat == null) {
      elapseFormat = SECOND_ELAPSE_FORMAT;
    }
    return String.format(elapseFormat, day, hour, minute, second, nanoElapseLocal);
  }

  /**
   * SQL language defined date time format.
   * */
  public static final DateTimeFormatter SQL_DATETIME_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

  /**
   * SQL language defined date format.
   * */
  public static final DateTimeFormatter SQL_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd");

  /**
   * @return a parsed {@link DateTime} object. Support both SQL date time format and SQL date format
   * @param datetime string
   * @throws IllegalArgumentException if the argument cannot be parsed.
   * */
  public static DateTime parse(final String datetime) throws IllegalArgumentException {
    try {
      return DateTime.parse(datetime, SQL_DATETIME_FORMAT);
    } catch (Throwable e) {
      try {
        return DateTime.parse(datetime, SQL_DATE_FORMAT);
      } catch (Throwable ee) {
        throw new IllegalArgumentException("Not a valid SQL datetime/date format, caused by: " + datetime);
      }
    }
  }

}
