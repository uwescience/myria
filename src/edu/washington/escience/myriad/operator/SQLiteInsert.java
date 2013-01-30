package edu.washington.escience.myriad.operator;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteJob;
import com.almworks.sqlite4java.SQLiteQueue;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.util.SQLiteUtils;

/**
 * An operator that inserts its tuples into a relation stored in a SQLite Database.
 * 
 * @author dhalperi
 * 
 */
public final class SQLiteInsert extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The SQLite Database that they will be inserted into. */
  private String pathToSQLiteDb;
  /** The name of the table the tuples should be inserted into. */
  private final String relationName;
  /** Whether to create the table or not. */
  private final boolean createTable;
  /** SQLite queue confines all SQLite operations to the same thread. */
  private SQLiteQueue queue;
  /** The statement used to insert tuples into the database. */
  private String insertString;

  /**
   * Constructs an insertion operator to store the tuples from the specified child in a SQLite database in the specified
   * file. This operator will only append to an existing database, or create and initialize one that does not exist.
   * 
   * @param child the source of tuples to be inserted.
   * @param relationName the name of the table the tuples should be inserted into.
   * @param pathToSQLiteDb the path to the database.
   * @param executor the ExecutorService for this process.
   */
  public SQLiteInsert(final Operator child, final String relationName, final String pathToSQLiteDb,
      final ExecutorService executor) {
    super(child, executor);
    Objects.requireNonNull(child);
    Objects.requireNonNull(relationName);
    this.pathToSQLiteDb = pathToSQLiteDb;
    this.relationName = relationName;
    createTable = false;
  }

  /**
   * Constructs an insertion operator to store the tuples from the specified child in a SQLite database in the specified
   * file. This operator will only append to an existing database, or create and initialize one that does not exist.
   * 
   * @param child the source of tuples to be inserted.
   * @param relationName the name of the table the tuples should be inserted into.
   * @param pathToSQLiteDb the path to the database.
   * @param executor the ExecutorService for this process.
   * @param createTable whether to create the table if the file already exists.
   */
  public SQLiteInsert(final Operator child, final String relationName, final String pathToSQLiteDb,
      final ExecutorService executor, final boolean createTable) {
    super(child, executor);
    Objects.requireNonNull(child);
    Objects.requireNonNull(relationName);
    this.pathToSQLiteDb = pathToSQLiteDb;
    this.relationName = relationName;
    this.createTable = createTable;
  }

  @Override
  public void cleanup() {
    try {
      queue.stop(true).join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  protected void consumeTuples(final TupleBatch tupleBatch) throws DbException {
    final SQLiteJob<Object> future = new SQLiteJob<Object>() {
      @Override
      protected Object job(final SQLiteConnection sqliteConnection) throws SQLiteException {
        /* BEGIN TRANSACTION */
        sqliteConnection.exec("BEGIN TRANSACTION");

        SQLiteStatement insertStatement = sqliteConnection.prepare(insertString);

        tupleBatch.getIntoSQLite(insertStatement);

        /* COMMIT TRANSACTION */
        sqliteConnection.exec("COMMIT TRANSACTION");

        insertStatement.dispose();
        insertStatement = null;

        return null;
      }
    };
    queue.execute(future);
    try {
      future.get();
      queue.flush();
    } catch (final InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (final ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void init() throws DbException {
    final File dbFile = new File(pathToSQLiteDb);

    /* Try and create a new file. */
    boolean created;
    try {
      created = dbFile.createNewFile();
    } catch (final IOException e) {
      throw new DbException(e);
    }

    /* Open a connection to that SQLite database. If creation succeeded, create a new table as well. */
    queue = new SQLiteQueue(dbFile);
    queue.start();
    if (created || createTable) {
      /* If succeeded, populate its schema. */
      queue.execute(new SQLiteJob<Integer>() {
        @Override
        protected Integer job(final SQLiteConnection connection) throws SQLiteException {
          connection.exec(SQLiteUtils.createStatementFromSchema(getSchema(), relationName));
          return null;
        }
      });
    }

    /* Set up the insert statement. */
    insertString = SQLiteUtils.insertStatementFromSchema(getSchema(), relationName);
  }

  /**
   * Needed for Worker.localizeQueryPlan.
   * 
   * @param pathToSQLiteDb the path to the database.
   */
  public void setPathToSQLiteDb(final String pathToSQLiteDb) {
    this.pathToSQLiteDb = pathToSQLiteDb;
  }

}
