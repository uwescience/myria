package edu.washington.escience.myriad.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myriad.operator.JdbcQueryScan;

public class JdbcAccessMethodTimingLocal {

  @Test
  public void test() throws DbException {
    /* TODO : make it auto run sql script before test to clear out database speedtest table */
    /* The number of trials */
    final int NTRIALS = 5;
    /* The number of tuples to write per trial */
    final int NUM_TUPLES = 1000000;

    /* Factor to multiply for conversion of nanoseconds to seconds */
    final double CONVERSION = 1000000000;

    /* Hardcoded connection information to run on MySQL on localhost */
    final String host = "localhost";
    final String user = "";
    final String password = "";
    final String dbms = "mysql";
    final String databaseName = "test";
    final String jdbcDriverName = "com.mysql.jdbc.Driver";

    /* Query information */
    final String query = "INSERT INTO speedtesttable VALUES(?, ?)";
    final String connectionString =
        "jdbc:" + dbms + "://" + host + "/" + databaseName + "?rewriteBatchedStatements=true";

    /* Create the tuple schema */
    Type[] types = new Type[] { Type.INT_TYPE, Type.STRING_TYPE };
    String[] columnNames = new String[] { "key", "value" };
    Schema schema = new Schema(types, columnNames);

    /* Create tuples and put them into a batch buffer */
    TupleBatchBuffer tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < NUM_TUPLES; i++) {
      tbb.put(0, i);
      tbb.put(1, (i + ""));
    }

    /* Count the number of tuples in the database originally */
    System.out.println("Counting number of tuples in database");
    long originalNumTuples = countNumberOfTuples(dbms, host, databaseName, jdbcDriverName, user, password);

    ArrayList<Long> durations = new ArrayList<Long>(NTRIALS);
    /* Connect to the database */
    try {
      final Connection jdbcConnection = DriverManager.getConnection(connectionString, user, password);

      /* Loop through N trials and insert the tuples */
      for (int i = 0; i < NTRIALS; i++) {
        System.out.println("Beginning insert trial # " + (i + 1));

        /* Get the list of batches from the buffer */
        List<TupleBatch> batches = tbb.getAll();
        /* Time the writing of all batches to the database */
        long startTime = System.nanoTime();
        for (TupleBatch batch : batches) {
          JdbcAccessMethod.tupleBatchInsert(jdbcConnection, query, batch);
        }
        long endTime = System.nanoTime();
        durations.add(endTime - startTime);
      }

      /* Close the database connection */
      jdbcConnection.close();

    } catch (SQLException e) {
      System.err.println(e.getMessage());
    }

    /* Count the number of tuples in the database after the insert and validate the number of tuples inserted */
    // TODO : Uncomment following lines for unit test of this speed test
    // System.out.println("Counting number of tuples in database\n");
    // long finalNumTuples = countNumberOfTuples(dbms, host, databaseName, jdbcDriverName, user, password);
    // assertTrue(finalNumTuples - originalNumTuples == NUM_TUPLES * NTRIALS);

    /* Calculate the timing results and print them out */
    double sum = 0;
    for (Long l : durations) {
      sum += l;
    }
    double seconds = sum / CONVERSION;

    /* Print out the time results */
    String message;
    if (sum > 0) {
      message = "Number of trials = " + NTRIALS;
      message += "\nTuple Batch Size = " + TupleBatch.BATCH_SIZE;
      message += "\nNumber of Tuples per trial = " + NUM_TUPLES;
      message += "\n";
      message += "\nTotal Number of Tuples = " + NUM_TUPLES * NTRIALS;
      message += "\nTotal time = " + seconds + " seconds";
      message += "\nTuples per second = " + (NUM_TUPLES * NTRIALS / seconds);
    } else {
      message = "No timing data received";
    }
    System.out.println(message);
  }

  private static long countNumberOfTuples(String dbms, String host, String databaseName, String jdbcDriverName,
      String user, String password) {
    long numTuples = 0;
    final String validateQuery = "select * from speedtesttable";
    final Type[] validateTypes = new Type[] { Type.INT_TYPE };
    final String[] validateColumnNames = new String[] { "value" };
    final Schema validateSchema = new Schema(validateTypes, validateColumnNames);
    final String validateConnectionString =
        "jdbc:" + dbms + "://" + host + "/" + databaseName + "?rewriteBatchedStatements=true";
    final JdbcQueryScan validateScan =
        new JdbcQueryScan(jdbcDriverName, validateConnectionString, validateQuery, validateSchema, user, password);

    try {
      validateScan.open();
      while (validateScan.hasNext()) {
        final TupleBatch vtb = (TupleBatch) validateScan.next();
        numTuples += vtb.numValidTuples();
      }
      validateScan.close();
      return numTuples;
    } catch (DbException e) {
      System.err.println(e.getMessage());
      return -1;
    }
  }
}
