package edu.washington.escience.myriad.parallel;

import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.washington.escience.myriad.Predicate;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myriad.accessmethod.SQLiteAccessMethod;
import edu.washington.escience.myriad.column.BooleanColumn;
import edu.washington.escience.myriad.column.DoubleColumn;
import edu.washington.escience.myriad.column.FloatColumn;
import edu.washington.escience.myriad.column.IntColumn;
import edu.washington.escience.myriad.column.LongColumn;
import edu.washington.escience.myriad.column.StringColumn;
import edu.washington.escience.myriad.proto.TransportProto.ColumnMessage;

/**
 * Runs some simple tests.
 * 
 * @author dhalperi, slxu
 * 
 */
public class Main {
  public static void main(String[] args) throws NoSuchElementException, DbException {
    JdbcTest();
    SQLiteTest();
    ProtoTest();
  };

  public static void ProtoTest() {
    ProtoTestBoolean();
    ProtoTestDouble();
    ProtoTestFloat();
    ProtoTestInt();
    ProtoTestLong();
    ProtoTestString();
  }

  private static void ProtoTestBoolean() {
    System.out.println("ProtoTestBoolean");
    BooleanColumn original = new BooleanColumn();
    original.put(true).put(false).put(true).put(false).put(false).put(false).put(false).put(false)
    .put(true).put(false).put(false).put(false).put(false).put(false);
    ColumnMessage serialized = original.serializeToProto();
    BooleanColumn deserialized = new BooleanColumn(serialized);
    System.out.println("original: " + original);
    System.out.println("deserialized: " + deserialized);
  }

  private static void ProtoTestDouble() {
    System.out.println("ProtoTestDouble");
    DoubleColumn original = new DoubleColumn();
    original.put(1).put(2).put(5).put(11);
    ColumnMessage serialized = original.serializeToProto();
    DoubleColumn deserialized = new DoubleColumn(serialized);
    System.out.println("original: " + original);
    System.out.println("deserialized: " + deserialized);
  }

  private static void ProtoTestFloat() {
    System.out.println("ProtoTestFloat");
    FloatColumn original = new FloatColumn();
    original.put(1).put(2).put(5).put(11);
    ColumnMessage serialized = original.serializeToProto();
    FloatColumn deserialized = new FloatColumn(serialized);
    System.out.println("original: " + original);
    System.out.println("deserialized: " + deserialized);
  }

  private static void ProtoTestInt() {
    System.out.println("ProtoTestInt");
    IntColumn original = new IntColumn();
    original.put(1).put(2).put(5).put(11);
    ColumnMessage serialized = original.serializeToProto();
    IntColumn deserialized = new IntColumn(serialized);
    System.out.println("original: " + original);
    System.out.println("deserialized: " + deserialized);
  }

  private static void ProtoTestLong() {
    System.out.println("ProtoTestLong");
    LongColumn original = new LongColumn();
    original.put(1).put(2).put(5).put(11);
    ColumnMessage serialized = original.serializeToProto();
    LongColumn deserialized = new LongColumn(serialized);
    System.out.println("original: " + original);
    System.out.println("deserialized: " + deserialized);
  }

  private static void ProtoTestString() {
    System.out.println("ProtoTestString");
    StringColumn original = new StringColumn();
    original.put("First").put("Second").put("Third").put("NextIsEmptyString").put("").put(
        "VeryVeryVeryVeryVeryVeryVeryVeryLongLast");
    ColumnMessage serialized = original.serializeToProto();
    StringColumn deserialized = new StringColumn(serialized);
    System.out.println("original: " + original);
    System.out.println("deserialized: " + deserialized);
  }

  public static void SQLiteTest() throws DbException {
    final String filename = "sql/sqlite.myriad_test/myriad_sqlite_test.db";
    final String query = "SELECT * FROM testtable";
    final String insert = "INSERT INTO testtable2 VALUES(?)";

    Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.OFF);

    /* Scan the testtable in database */
    SQLiteQueryScan scan = new SQLiteQueryScan(filename, query);

    /* Filter on first column INTEGER >= 50 */
    Filter filter1 = new Filter(Predicate.Op.GREATER_THAN_OR_EQ, 0, new Long(50), scan);
    /* Filter on first column INTEGER <= 60 */
    Filter filter2 = new Filter(Predicate.Op.LESS_THAN_OR_EQ, 0, new Long(60), filter1);

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
      TupleBatch tb = root.next();
      System.out.println(tb);
      SQLiteAccessMethod.tupleBatchInsert(filename, insert, tb);
    }

    /* Cleanup */
    root.close();
  }

  public static void JdbcTest() throws DbException {
    final String host = "dsp.cs.washington.edu";
    final int port = 3306;
    final String user = "myriad";
    final String password = "nays26[shark";
    final String dbms = "mysql";
    final String databaseName = "myriad_test";
    final String jdbcDriverName = "com.mysql.jdbc.Driver";
    final String query = "select * from testtable";
    final String insert = "INSERT INTO testtable2 VALUES(?)";

    String connectionString =
        "jdbc:" + dbms + "://" + host + ":" + port + "/" + databaseName + "?user=" + user
        + "&password=" + password;
    JdbcQueryScan scan = new JdbcQueryScan(jdbcDriverName, connectionString, query);
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
      TupleBatch tb = root.next();
      System.out.println(tb);
      JdbcAccessMethod.tupleBatchInsert(jdbcDriverName, connectionString, insert, tb);
    }

    root.close();
  }

  /** Inaccessible. */
  private Main() {
  }
}
