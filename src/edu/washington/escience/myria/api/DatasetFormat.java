package edu.washington.escience.myria.api;

/**
 * Myria REST API supported dataset formats.
 * */
public enum DatasetFormat {

  /**
   * CSV.
   * */
  CSV() {
    @Override
    public String toString() {
      return "csv";
    }

    @Override
    public Character getColumnDelimiter() {
      return ',';
    }
  },

  /**
   * TSV.
   * */
  TSV() {
    @Override
    public String toString() {
      return "tsv";
    }

    @Override
    public Character getColumnDelimiter() {
      return '\t';
    }
  },

  /**
   * JSON.
   * */
  JSON() {
    @Override
    public String toString() {
      return "json";
    }
  };

  /**
   * @return delimiter of columns.
   * */
  public Character getColumnDelimiter() {
    return null;
  }

  /**
   * Helper function to parse a format string, with default value "csv".
   * 
   * @param format the format string, with default value "csv".
   * @return the cleaned-up format string.
   */
  public static DatasetFormat validateFormat(final String format) {
    String cleanFormat = format;
    if (cleanFormat == null) {
      cleanFormat = "csv";
    }
    cleanFormat = cleanFormat.trim().toLowerCase();
    /* CSV is legal */
    if (cleanFormat.equals("csv")) {
      return CSV;
    }
    /* TSV is legal */
    if (cleanFormat.equals("tsv")) {
      return TSV;
    }
    /* JSON is legal */
    if (cleanFormat.equals("json")) {
      return JSON;
    }
    return null;
  }
}
