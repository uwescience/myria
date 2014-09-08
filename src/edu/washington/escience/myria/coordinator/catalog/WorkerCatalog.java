package edu.washington.escience.myria.coordinator.catalog;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.parallel.SocketInfo;

/**
 * This class is intended to store the configuration and catalog information for a Myria worker.
 *
 *
 */
public final class WorkerCatalog {
  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkerCatalog.class);

  /**
   * @param filename the path to the SQLite database storing the worker catalog.
   * @return a fresh WorkerCatalog fitting the specified description.
   * @throws CatalogException if there is an error creating the database or the file already exists.
   *
   *           TODO add some sanity checks to the filename?
   */
  public static WorkerCatalog create(final String filename) throws CatalogException {
    Objects.requireNonNull(filename);
    return WorkerCatalog.create(filename, false);
  }

  /**
   * @param filename the path to the SQLite database storing the catalog.
   * @param overwrite specifies whether to overwrite an existing WorkerCatalog.
   * @return a fresh WorkerCatalog fitting the specified description.
   * @throws CatalogException if the database already exists, or there is an error creating it.
   *
   *           TODO add some sanity checks to the filename?
   */
  public static WorkerCatalog create(final String filename, final boolean overwrite) throws CatalogException {
    Objects.requireNonNull(filename);

    /* if overwrite is false, error if the file exists. */
    final File catalogFile = new File(filename);
    if (!overwrite && catalogFile.exists()) {
      throw new CatalogException(filename + " already exists");
    }
    return WorkerCatalog.createFromFile(catalogFile);
  }

  /**
   *
   * @param catalogFile a File object pointing to the SQLite database that will store the WorkerCatalog. If catalogFile
   *          is null, this creates an in-memory SQLite database.
   * @return a fresh WorkerCatalog fitting the specified description.
   * @throws CatalogException if there is an error opening the database.
   *
   *           TODO add some sanity checks to the filename?
   */
  private static WorkerCatalog createFromFile(final File catalogFile) throws CatalogException {
    /* Connect to the database. */
    final SQLiteConnection sqliteConnection = new SQLiteConnection(catalogFile);
    try {
      sqliteConnection.open();
    } catch (final SQLiteException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(e.toString());
      }
      throw new CatalogException("SQLiteException while creating the new WorkerCatalog", e);
    }

    /* Create all the tables in the WorkerCatalog. */
    try {
      /* @formatter:off */
      sqliteConnection.exec("PRAGMA journal_mode = WAL;");
      sqliteConnection.exec("BEGIN TRANSACTION");
      sqliteConnection.exec("DROP TABLE IF EXISTS configuration");
      sqliteConnection.exec(
          "CREATE TABLE configuration (\n"
              + "    key STRING UNIQUE NOT NULL,\n"
              + "    value STRING NOT NULL);");
      sqliteConnection.exec("DROP TABLE IF EXISTS masters");
      sqliteConnection.exec(
          "CREATE TABLE masters (\n"
              + "    master_id INTEGER PRIMARY KEY ASC,\n"
              + "    host_port STRING NOT NULL);");
      sqliteConnection.exec("DROP TABLE IF EXISTS workers");
      sqliteConnection.exec(
          "CREATE TABLE workers (\n"
              + "    worker_id INTEGER PRIMARY KEY ASC,\n"
              + "    host_port STRING NOT NULL);");
      sqliteConnection.exec("DROP TABLE IF EXISTS relations");
      sqliteConnection.exec(
          "CREATE TABLE relations (\n"
              + "    relation_id INTEGER PRIMARY KEY ASC,\n"
              + "    relation_name STRING NOT NULL UNIQUE);");
      sqliteConnection.exec("DROP TABLE IF EXISTS relation_schema");
      sqliteConnection.exec(
          "CREATE TABLE relation_schema (\n"
              + "    relation_id INTEGER NOT NULL REFERENCES relations(relation_id),\n"
              + "    col_index INTEGER NOT NULL,\n"
              + "    col_name STRING,\n"
              + "    col_type STRING NOT NULL);");
      sqliteConnection.exec("DROP TABLE IF EXISTS shards");
      sqliteConnection.exec(
          "CREATE TABLE shards (\n"
              + "    stored_relation_id INTEGER NOT NULL REFERENCES stored_relations(stored_relation_id),\n"
              + "    shard_index INTEGER NOT NULL,\n"
              + "    location STRING NOT NULL);");
      sqliteConnection.exec("COMMIT TRANSACTION");
      /* @formatter:on*/
    } catch (final SQLiteException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(e.toString());
      }
      try {
        sqliteConnection.exec("ROLLBACK TRANSACTION");
      } catch (final SQLiteException e1) {
        assert true;
        /* Ignore failed rollback, we're throwing an exception anyway. */
      }
      throw new CatalogException("SQLiteException while creating new WorkerCatalog tables", e);
    }

    try {
      return new WorkerCatalog(sqliteConnection);
    } catch (final SQLiteException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(e.toString());
      }
      throw new CatalogException("SQLiteException while creating WorkerCatalog from SQLiteConnection", e);
    }
  }

  /**
   * @return a fresh WorkerCatalog fitting the specified description.
   * @throws CatalogException if there is an error opening the database.
   *
   *           TODO add some sanity checks to the filename?
   */
  public static WorkerCatalog createInMemory() throws CatalogException {
    return WorkerCatalog.createFromFile(null);
  }

  /**
   * Opens the worker catalog stored as a SQLite database in the specified file.
   *
   * @param filename the path to the SQLite database storing the catalog.
   * @return an initialized WorkerCatalog object ready to be used for experiments.
   * @throws FileNotFoundException if the given file does not exist.
   * @throws CatalogException if there is an error connecting to the database.
   *
   *           TODO add some sanity checks to the filename?
   */
  public static WorkerCatalog open(final String filename) throws FileNotFoundException, CatalogException {
    Objects.requireNonNull(filename);

    /* See if the file exists, and create it if not. */
    final File catalogFile = new File(filename);
    if (!catalogFile.exists()) {
      throw new FileNotFoundException(filename);
    }

    /* Connect to the database */
    final SQLiteConnection sqliteConnection = new SQLiteConnection(catalogFile);
    try {
      sqliteConnection.open(false);

      return new WorkerCatalog(sqliteConnection);
    } catch (final SQLiteException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(e.toString());
      }
      throw new CatalogException(e);
    }
  }

  /** Is the WorkerCatalog closed? */
  private boolean isClosed = true;

  /** The connection to the SQLite database that stores the WorkerCatalog. */
  private SQLiteConnection sqliteConnection;

  /**
   * Not publicly accessible.
   *
   * @param sqliteConnection connection to the SQLite database that stores the WorkerCatalog.
   * @throws SQLiteException if there is an error turning on foreign keys.
   */
  private WorkerCatalog(final SQLiteConnection sqliteConnection) throws SQLiteException {
    Objects.requireNonNull(sqliteConnection);
    this.sqliteConnection = sqliteConnection;
    isClosed = false;
    sqliteConnection.exec("PRAGMA foreign_keys = ON;");
  }

  /**
   * Adds a master using the specified host and port to the WorkerCatalog.
   *
   * @param hostPortString specifies the path to the master in the format "host:port"
   * @return this WorkerCatalog
   * @throws CatalogException if the hostPortString is invalid or there is a database exception.
   */
  public WorkerCatalog addMaster(final String hostPortString) throws CatalogException {
    Objects.requireNonNull(hostPortString);
    if (isClosed) {
      throw new CatalogException("WorkerCatalog is closed.");
    }
    try {
      @SuppressWarnings("unused")
      /* Just used to verify that hostPortString is legal */
      final SocketInfo sockInfo = SocketInfo.valueOf(hostPortString);
      final SQLiteStatement statement = sqliteConnection.prepare("INSERT INTO masters(host_port) VALUES(?);", false);
      statement.bind(1, hostPortString);
      statement.step();
      statement.dispose();
    } catch (final SQLiteException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(e.toString());
      }
      throw new CatalogException(e);
    }
    return this;
  }

  /**
   * Adds the metadata for a relation into the WorkerCatalog.
   *
   * @param name the name of the relation.
   * @param schema the schema of the relation.
   * @throws CatalogException if the relation is already in the WorkerCatalog or there is an error in the database.
   *
   *           TODO if we use SQLite in a multithreaded way this will need to be revisited.
   *
   */
  public void addRelationMetadata(final String name, final Schema schema) throws CatalogException {
    Objects.requireNonNull(name);
    Objects.requireNonNull(schema);
    try {
      /* To begin: start a transaction. */
      sqliteConnection.exec("BEGIN TRANSACTION;");

      /* First, insert the relation name. */
      SQLiteStatement statement = sqliteConnection.prepare("INSERT INTO relations (relation_name) VALUES (?);");
      statement.bind(1, name);
      statement.stepThrough();
      statement.dispose();
      statement = null;

      /* Second, figure out what ID this relation was assigned. */
      final long relationId = sqliteConnection.getLastInsertId();

      /* Third, populate the Schema table. */
      statement =
          sqliteConnection.prepare("INSERT INTO relation_schema(relation_id, col_index, col_name, col_type) "
              + "VALUES (?,?,?,?);");
      statement.bind(1, relationId);
      for (int i = 0; i < schema.numColumns(); ++i) {
        statement.bind(2, i);
        statement.bind(3, schema.getColumnName(i));
        statement.bind(4, schema.getColumnType(i).toString());
        statement.step();
        statement.reset(false);
      }
      statement.dispose();
      statement = null;

      /* To complete: commit the transaction. */
      sqliteConnection.exec("COMMIT TRANSACTION;");
    } catch (final SQLiteException e) {
      try {
        sqliteConnection.exec("ABORT TRANSACTION;");
      } catch (final SQLiteException e2) {
        assert true; /* Do nothing. */
      }
      throw new CatalogException(e);
    }
  }

  /**
   * Adds a worker using the specified host and port to the WorkerCatalog.
   *
   * @param workerId specifies the global identifier of this worker.
   * @param hostPortString specifies the path to the worker in the format "host:port".
   * @return this WorkerCatalog.
   * @throws CatalogException if the hostPortString is invalid or there is a database exception.
   */
  public WorkerCatalog addWorker(final int workerId, final String hostPortString) throws CatalogException {
    Objects.requireNonNull(hostPortString);
    if (isClosed) {
      throw new CatalogException("WorkerCatalog is closed.");
    }
    try {
      @SuppressWarnings("unused")
      /* Just used to verify that hostPortString is legal */
      final SocketInfo sockInfo = SocketInfo.valueOf(hostPortString);
      final SQLiteStatement statement =
          sqliteConnection.prepare("INSERT INTO workers(worker_id, host_port) VALUES(?,?);", false);
      statement.bind(1, workerId);
      statement.bind(2, hostPortString);
      statement.step();
      statement.dispose();
    } catch (final SQLiteException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(e.toString());
      }
      throw new CatalogException(e);
    }
    return this;
  }

  /**
   * Adds workers in batch.
   *
   * @param id2HostPortString worker id to host:port string.
   * @return this WorkerCatalog.
   * @throws CatalogException if the hostPortString is invalid or there is a database exception.
   */
  public WorkerCatalog addWorkers(final Map<String, String> id2HostPortString) throws CatalogException {
    if (isClosed) {
      throw new CatalogException("WorkerCatalog is closed.");
    }
    try {
      final SQLiteStatement statement =
          sqliteConnection.prepare("INSERT INTO workers(worker_id, host_port) VALUES(?,?);", false);
      sqliteConnection.exec("BEGIN TRANSACTION");
      for (Map.Entry<String, String> e : id2HostPortString.entrySet()) {
        @SuppressWarnings("unused")
        final SocketInfo sockInfo = SocketInfo.valueOf(e.getValue());
        statement.bind(1, Integer.valueOf(e.getKey()));
        statement.bind(2, e.getValue());
        statement.step();
        statement.reset(false);
      }

      sqliteConnection.exec("COMMIT TRANSACTION");
      statement.dispose();
    } catch (final SQLiteException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(e.toString());
      }
      try {
        /* Commit transaction. */
        sqliteConnection.exec("ROLLBACK TRANSACTION");
      } catch (SQLiteException e1) {
        /* Still throw the original exception */
        throw new CatalogException(e);
      }
      throw new CatalogException(e);
    }
    return this;
  }

  /**
   * Close the connection to the database that stores the WorkerCatalog. Idempotent. Calling any methods (other than
   * close()) on this WorkerCatalog will throw a CatalogException.
   */
  public void close() {
    if (sqliteConnection != null) {
      sqliteConnection.dispose();
      sqliteConnection = null;
    }
    isClosed = true;
  }

  /**
   * Extract the value of a particular configuration parameter from the database. Returns null if the parameter is not
   * configured.
   *
   * @param key the name of the configuration parameter.
   * @return the value of the configuration parameter, or null if that configuration is not supported.
   * @throws CatalogException if there is an error in the backing database.
   */
  public String getConfigurationValue(final String key) throws CatalogException {
    Objects.requireNonNull(key);
    if (isClosed) {
      throw new CatalogException("WorkerCatalog is closed.");
    }
    try {
      /* Getting this out is a simple query, which does not need to be cached. */
      final SQLiteStatement statement =
          sqliteConnection.prepare("SELECT value FROM configuration WHERE key=? LIMIT 1;", false).bind(1, key);
      if (!statement.step()) {
        /* If step() returns false, there's no data. Return null. */
        return null;
      }
      final String ret = statement.columnString(0);
      statement.dispose();
      return ret;
    } catch (final SQLiteException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(e.toString());
      }
      throw new CatalogException(e);
    }
  }

  /**
   * @return all the configurations.
   * @throws CatalogException if error occurs in catalog parsing.
   */
  public ImmutableMap<String, String> getAllConfigurations() throws CatalogException {
    if (isClosed) {
      throw new CatalogException("WorkerCatalog is closed.");
    }
    HashMap<String, String> configurations = new HashMap<String, String>();
    try {
      /* Getting this out is a simple query, which does not need to be cached. */
      final SQLiteStatement statement = sqliteConnection.prepare("SELECT * FROM configuration;", false);
      while (statement.step()) {
        configurations.put(statement.columnString(0), statement.columnString(1));
      }
      statement.dispose();
      return ImmutableMap.copyOf(configurations);
    } catch (final SQLiteException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(e.toString());
      }
      throw new CatalogException(e);
    }
  }

  /**
   * @return the set of masters stored in this WorkerCatalog.
   * @throws CatalogException if there is an error in the database.
   */
  public List<SocketInfo> getMasters() throws CatalogException {
    if (isClosed) {
      throw new CatalogException("WorkerCatalog is closed.");
    }

    final ArrayList<SocketInfo> masters = new ArrayList<SocketInfo>();
    try {
      final SQLiteStatement statement = sqliteConnection.prepare("SELECT * FROM masters;", false);
      while (statement.step()) {
        masters.add(SocketInfo.valueOf(statement.columnString(1)));
      }
      statement.dispose();
    } catch (final SQLiteException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(e.toString());
      }
      throw new CatalogException(e);
    }

    return masters;
  }

  /**
   * @return the set of workers stored in this WorkerCatalog.
   * @throws CatalogException if there is an error in the database.
   */
  public Map<Integer, SocketInfo> getWorkers() throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    final ConcurrentHashMap<Integer, SocketInfo> workers = new ConcurrentHashMap<Integer, SocketInfo>();

    try {
      final SQLiteStatement statement = sqliteConnection.prepare("SELECT * FROM workers;", false);
      while (statement.step()) {
        workers.put(statement.columnInt(0), SocketInfo.valueOf(statement.columnString(1)));
      }
      statement.dispose();
    } catch (final SQLiteException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(e.toString());
      }
      throw new CatalogException(e);
    }

    return workers;
  }

  /**
   * Extract the value of a particular configuration parameter from the database. Returns null if the parameter is not
   * configured.
   *
   * @param key the name of the configuration parameter.
   * @param value the value of the configuration parameter.
   * @throws CatalogException if there is an error in the backing database.
   */
  public void setConfigurationValue(final String key, final String value) throws CatalogException {
    Objects.requireNonNull(key);
    if (isClosed) {
      throw new CatalogException("WorkerCatalog is closed.");
    }
    try {
      /* Getting this out is a simple query, which does not need to be cached. */
      final SQLiteStatement statement =
          sqliteConnection.prepare("INSERT INTO configuration VALUES(?,?);", false).bind(1, key).bind(2, value);
      statement.step();
      statement.dispose();
    } catch (final SQLiteException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(e.toString());
      }
      throw new CatalogException(e);
    }
  }

  /**
   * Set all the configuration values in the provided map in a single transaction.
   *
   * @param entries the value of the configuration parameter.
   * @throws CatalogException if there is an error in the backing database.
   */
  public void setAllConfigurationValues(final Map<String, String> entries) throws CatalogException {
    Objects.requireNonNull(entries);
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }
    try {
      /* Start transaction. */
      sqliteConnection.exec("BEGIN TRANSACTION");
      final SQLiteStatement statement = sqliteConnection.prepare("INSERT INTO configuration VALUES(?,?);", false);
      for (Map.Entry<String, String> entry : entries.entrySet()) {
        if (entry.getValue() == null) {
          continue;
        }
        statement.bind(1, entry.getKey());
        statement.bind(2, entry.getValue());
        statement.step();
        statement.reset(false);
      }
      /* Commit transaction. */
      sqliteConnection.exec("COMMIT TRANSACTION");
      statement.dispose();
    } catch (final SQLiteException e) {
      if (LOGGER.isErrorEnabled()) {
        LOGGER.error(e.toString());
      }
      try {
        /* Commit transaction. */
        sqliteConnection.exec("ROLLBACK TRANSACTION");
      } catch (SQLiteException e1) {
        /* Still throw the original exception */
        throw new CatalogException(e);
      }
      throw new CatalogException(e);
    }
  }
}
