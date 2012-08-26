package edu.washington.escience.parallel;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import edu.washington.escience.Predicate;
import edu.washington.escience.Schema;
import edu.washington.escience.Type;

public class Main {
  public static void JdbcTest() throws NoSuchElementException, DbException {
    final String host = "dsp.cs.washington.edu";
    final int port = 3306;
    final String user = "myriad";
    final String password = "nays26[shark";
    final String dbms = "mysql";
    final String databaseName = "myriad_test";

    String connectionString =
        "jdbc:" + dbms + "://" + host + ":" + port + "/" + databaseName + "?user=" + user
        + "&password=" + password;
    JdbcQueryScan scan =
        new JdbcQueryScan("com.mysql.jdbc.Driver", connectionString, "select * from testtable");
    Filter filter1 = new Filter(Predicate.Op.GREATER_THAN_OR_EQ, 0, new Integer(50), scan);

    Filter filter2 = new Filter(Predicate.Op.LESS_THAN_OR_EQ, 0, new Integer(60), filter1);

    ArrayList<Integer> fieldIdx = new ArrayList<Integer>();
    fieldIdx.add(1);
    ArrayList<Type> fieldType = new ArrayList<Type>();
    fieldType.add(Type.STRING_TYPE);

    Project project = new Project(fieldIdx, fieldType, filter2);

    Operator root = project;

    root.open();

    Schema schema = root.getSchema();

    if (schema != null) {
      System.out.println("Schema of result is: " + schema);
    } else {
      System.err.println("Result has no Schema, exiting");
      root.close();
      return;
    }

    while (root.hasNext()) {
      System.out.println(root.next());
    }

    root.close();
  }

  public static void SQLiteTest() throws DbException {
    final String filename = "sql/sqlite.myriad_test/myriad_sqlite_test.db";
    final String query = "select * from testtable";

    /* Scan the testtable in database */
    SQLiteQueryScan scan = new SQLiteQueryScan(filename, query);

    /* Filter on first column INTEGER >= 50 */
    Filter filter1 = new Filter(Predicate.Op.GREATER_THAN_OR_EQ, 0, new Integer(50), scan);
    /* Filter on first column INTEGER <= 60 */
    Filter filter2 = new Filter(Predicate.Op.LESS_THAN_OR_EQ, 0, new Integer(60), filter1);

    /* Project onto second column STRING */
    ArrayList<Integer> fieldIdx = new ArrayList<Integer>();
    fieldIdx.add(1);
    ArrayList<Type> fieldType = new ArrayList<Type>();
    fieldType.add(Type.STRING_TYPE);
    Project project = new Project(fieldIdx, fieldType, filter2);

    /* Project is the output operator */
    Operator root = project;
    root.open();

    /* For debugging purposes, print Schema */
    Schema schema = root.getSchema();
    if (schema != null) {
      System.out.println("Schema of result is: " + schema);
    } else {
      System.err.println("Result has no Schema, exiting");
      root.close();
      return;
    }

    /* Print all the results */
    while (root.hasNext()) {
      System.out.println(root.next());
    }

    /* Cleanup */
    root.close();
  }

  public static void main(String[] args) throws NoSuchElementException, DbException {
    JdbcTest();
    SQLiteTest();
  }
}
