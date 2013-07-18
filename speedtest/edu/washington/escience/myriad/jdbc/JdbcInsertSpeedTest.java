package edu.washington.escience.myriad.jdbc;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.TupleBatchBuffer;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myriad.accessmethod.JdbcInfo;
import edu.washington.escience.myriad.operator.QueryScan;
import edu.washington.escience.myriad.util.JdbcUtils;

/**
 * Test the insertion speed of specified databases.
 * 
 * TODO : Currently to test different batch sizes, the constant TupleBatch.BATCH_SIZE must be altered manually. It would
 * be nice to alter that so that this test (or another one) could go through different batch sizes to find the optimal
 * size for each database.
 * 
 * @author aarond79
 */
public class JdbcInsertSpeedTest {

  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Test Constants - vary these to alter the test
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /**
   * A simple class that holds the database connection data and runs the speed test on it's own database.
   * 
   * @author aarond79
   */
  private final class SpeedTestData {

    // Identifiers for database that is being tested
    private final JdbcInfo jdbcInfo;
    private final String table;

    // Total time received
    private double sum = 0;
    private int numSuccessfulTrials = 0;

    // Error information
    private boolean error = false;
    private String errorMessage = null;

    /**
     * Construct the object.
     * 
     * @param host
     * @param jdbcDriverName
     * @param user
     * @param password
     * @param connectionString
     * @param table
     */
    private SpeedTestData(final JdbcInfo jdbcInfo, final String table) {
      this.jdbcInfo = jdbcInfo;
      this.table = table;
    }

    /**
     * Counts the number of tuples in the database table.
     * 
     * @param countQuery
     * @return
     * @throws InterruptedException
     */
    private long countNumberOfTuples(final String countQuery) throws InterruptedException {
      final ImmutableList<Type> countTypes = ImmutableList.of(Type.INT_TYPE);
      final ImmutableList<String> countColumnNames = ImmutableList.of("value");
      final Schema countSchema = new Schema(countTypes, countColumnNames);
      final QueryScan validateScan = new QueryScan(countQuery, countSchema);

      try {
        validateScan.open(null);
        final TupleBatch vtb = validateScan.nextReady();
        if (vtb != null) {
          return vtb.getLong(0, 0);
        } else {
          throw new DbException("Error reading results from query: " + countQuery);
        }
      } catch (final DbException e) {
        System.err.println(e.getMessage());
        return -1;
      }
    }

    public void runTest() {
      System.out.println("testing: " + jdbcInfo.getHost() + ", using " + jdbcInfo.getDriverClass());
      // Create the queries
      final String dropQuery = "DROP TABLE " + table;
      final String createQuery =
          "CREATE TABLE " + table + " (k " + JdbcUtils.typeToDbmsType(Type.INT_TYPE, jdbcInfo.getDbms()) + ", v "
              + JdbcUtils.typeToDbmsType(Type.STRING_TYPE, jdbcInfo.getDbms()) + ")";
      final String insertQuery = "INSERT INTO " + table + " VALUES(?, ?)";
      final String countQuery = "SELECT COUNT(*) FROM " + table;

      JdbcAccessMethod jdbcAccessMethod;
      try {
        jdbcAccessMethod = new JdbcAccessMethod(jdbcInfo, false);
      } catch (final DbException e) {
        error = true;
        errorMessage = e.getMessage();
        return;
      }

      try {
        jdbcAccessMethod.execute(dropQuery);
      } catch (DbException e) {
        /* Okay, pass. */
      }

      try {
        jdbcAccessMethod.execute(createQuery);

        // Loop through N trials and insert the tuples
        for (int i = 0; i < NTRIALS; i++) {

          // Count the number of tuples in the table already
          final long origNumberTuples = countNumberOfTuples(countQuery);
          if (origNumberTuples < 0) {
            throw new Exception("Error reading number of tuples in table.");
          }

          // Get the list of batches from the buffer
          final List<TupleBatch> batches = tbb.getAll();
          // Time the write operation
          final long startTime = System.nanoTime();
          for (final TupleBatch batch : batches) {
            jdbcAccessMethod.tupleBatchInsert(insertQuery, batch);
          }
          final long endTime = System.nanoTime();

          // Count the number of tuples in the table afterwards
          final long finalNumberTuples = countNumberOfTuples(countQuery);
          if (finalNumberTuples < 0) {
            throw new Exception("Error reading number of tuples in table.");
          }

          if (finalNumberTuples == origNumberTuples + NUM_TUPLES) {
            // Add to the sum of writes
            sum += endTime - startTime;
            numSuccessfulTrials++;
          } else {
            // This insertion had an error, do not add it to the sum
          }
        }

        if (numSuccessfulTrials <= 0) {
          error = true;
          errorMessage = "No successful trials";
        }

        // Close the database connection
        jdbcAccessMethod.close();

      } catch (final DbException e) {
        error = true;
        errorMessage = e.getMessage();
      } catch (final Exception e) {
        error = true;
        errorMessage = e.getMessage();
      }
    }

    @Override
    public String toString() {
      if (error) {
        return errorMessage;
      }

      // Calculate the total time (in seconds)
      final double seconds = sum / CONVERSION;

      // Create the result message
      final StringBuilder sb = new StringBuilder("");
      sb.append("Results");
      sb.append("\n\tNumber of trials = " + NTRIALS);
      sb.append("\n\tTuple batch size = " + TupleBatch.BATCH_SIZE); // TODO : implement ability to alter batch sizes?
      sb.append("\n\tTuples per trial = " + NUM_TUPLES);
      sb.append("\n\n\tTotal time = " + String.format("%1$,.2f", seconds) + " seconds");
      sb.append("\n\tTuples per second = " + String.format("%1$,.2f", (NUM_TUPLES * numSuccessfulTrials / seconds)));
      return sb.toString();
    }
  }

  // the number of trials per speed test (higher number gives more accuracy, slower execution)
  private final static int NTRIALS = 5;

  // The total number of tuples to write for each trial
  private final static int NUM_TUPLES = 100000;

  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Test Variables - for test internal use only
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  // Used to convert to seconds from nanoseconds
  private final static double CONVERSION = 1000000000;

  // The tuples to be used for the test
  private TupleBatchBuffer tbb = null;

  // The list of tests to run
  private final List<SpeedTestData> tests = new ArrayList<SpeedTestData>();

  @Test
  public void runSpeedtest() {
    System.out.println("Timing Insertion Speed:");
    for (final SpeedTestData test : tests) {
      System.out.println();
      test.runTest();
      System.out.println(test);
    }
  }

  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  // Private helper classes
  // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  /**
   * Sets up the test structures and data.
   * 
   * TODO : In the section with database names, connections, etc. you can input the connection and table data for each
   * database to speed test.
   */
  @Before
  public void setup() {
    // Create the test tuple schema
    final ImmutableList<Type> types = ImmutableList.of(Type.INT_TYPE, Type.STRING_TYPE);
    final ImmutableList<String> columnNames = ImmutableList.of("key", "value");
    final Schema schema = new Schema(types, columnNames);

    // Create tuples and put them into the batch buffer
    tbb = new TupleBatchBuffer(schema);
    for (int i = 0; i < NUM_TUPLES; i++) {
      tbb.put(0, i);
      tbb.put(1, (i + ""));
    }

    String host;
    int port;
    String user;
    String password;
    String dbms;
    String databaseName;
    String jdbcDriverName;
    String table;
    JdbcInfo jdbcInfo;

    // Set up connection data for the databases to be tested

    host = "54.245.108.198";
    port = 50000;
    user = "myria";
    password = "nays26[shark";
    dbms = "monetdb";
    databaseName = "myria-test";
    jdbcDriverName = "nl.cwi.monetdb.jdbc.MonetDriver";
    jdbcInfo = JdbcInfo.of(jdbcDriverName, dbms, host, port, databaseName, user, password);
    table = "speedtesttable";
    tests.add(new SpeedTestData(jdbcInfo, table));

    host = "54.245.108.198";
    port = 3306;
    user = "myriad";
    password = "nays26[shark";
    dbms = "mysql";
    databaseName = "myriad_test";
    jdbcDriverName = "com.mysql.jdbc.Driver";
    Properties properties = new Properties();
    properties.setProperty("rewriteBatchedStatements", "true");
    jdbcInfo = JdbcInfo.of(jdbcDriverName, dbms, host, port, databaseName, user, password, properties);
    table = "speedtesttable";
    tests.add(new SpeedTestData(jdbcInfo, table));

    host = "localhost";
    user = "";
    password = "";
    dbms = "mysql";
    databaseName = "test";
    jdbcDriverName = "com.mysql.jdbc.Driver";
    jdbcInfo = JdbcInfo.of(jdbcDriverName, dbms, host, port, databaseName, user, password);
    table = "speedtesttable";
    /*
     * connectionString = "jdbc:" + dbms + "://" + host + "/" + databaseName + "?rewriteBatchedStatements=true";
     */
    tests.add(new SpeedTestData(jdbcInfo, table));
  }
}
