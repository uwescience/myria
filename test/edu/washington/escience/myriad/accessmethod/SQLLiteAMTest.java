package edu.washington.escience.myriad.accessmethod;

import java.io.File;

import org.junit.Test;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myriad.Schema;

public class SQLLiteAMTest {

  @Test
  public void sqliteEmptyTest() throws SQLiteException {
    final SQLiteConnection sqliteConnection = new SQLiteConnection(new File("/tmp/test/emptytable.db"));
    sqliteConnection.open(false);

    /* Set up and execute the query */
    final SQLiteStatement statement = sqliteConnection.prepare("select * from empty");

    /* Step the statement once so we can figure out the Schema */
    statement.step();
    try {
      if (!statement.hasStepped()) {
        statement.step();
      }
      System.out.println(Schema.fromSQLiteStatement(statement));
    } catch (final SQLiteException e) {
      throw new RuntimeException(e.getMessage());
    }
  }
}
