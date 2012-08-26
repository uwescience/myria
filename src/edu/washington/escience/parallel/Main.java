package edu.washington.escience.parallel;

import java.util.ArrayList;
import java.util.NoSuchElementException;

import edu.washington.escience.Predicate;
import edu.washington.escience.Schema;
import edu.washington.escience.Type;

public class Main {
  final static String host = "dsp.cs.washington.edu";
  final static int port = 3306;
  final static String user = "myriad";
  final static String password = "nays26[shark";
  final static String dbms = "mysql";
  final static String databaseName = "myriad_test";

  public static void main(String[] args) throws NoSuchElementException, DbException {
    String connectionString =
        "jdbc:" + dbms + "://" + host + ":" + port + "/" + databaseName + "?user=" + user
        + "&password=" + password;
    JdbcSeqScan scan =
        new JdbcSeqScan("com.mysql.jdbc.Driver", connectionString, "select * from testtable");
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
    } else
      return;

    while (root.hasNext())
      System.out.println(root.next());
  }
}
