package edu.washington.escience.myriad.coordinator.catalog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteJob;
import com.almworks.sqlite4java.SQLiteQueue;
import com.almworks.sqlite4java.SQLiteStatement;

import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.parallel.SocketInfo;

/**
 * This class is intended to store the configuration information for a Myriad installation.
 * 
 * @author dhalperi
 * 
 */
public final class Catalog {
  /** The logger for this class. Defaults to myriad level, but could be set to a finer granularity if needed. */
  private static final Logger LOGGER = LoggerFactory.getLogger("edu.washington.escience.myriad");

  /**
   * @param filename the path to the SQLite database storing the catalog.
   * @param description specifies a description for the configuration stored in this Catalog.
   * @return a fresh Catalog fitting the specified description.
   * @throws IOException if the specified file already exists.
   * @throws CatalogException if there is an error opening the database.
   * 
   *           TODO add some sanity checks to the filename?
   */
  public static Catalog create(final String filename, final String description) throws IOException, CatalogException {
    Objects.requireNonNull(filename);
    Objects.requireNonNull(description);
    return Catalog.create(filename, description, false);
  }

  /**
   * @param filename the path to the SQLite database storing the catalog.
   * @param description specifies a description for the configuration stored in this Catalog.
   * @param overwrite specifies whether to overwrite an existing Catalog.
   * @return a fresh Catalog fitting the specified description.
   * @throws IOException if overwrite is true and the specified file already exists.
   * @throws CatalogException if there is an error opening the database.
   * 
   *           TODO add some sanity checks to the filename?
   */
  public static Catalog create(final String filename, final String description, final boolean overwrite)
      throws IOException, CatalogException {
    Objects.requireNonNull(filename);
    Objects.requireNonNull(description);

    /* if overwrite is false, error if the file exists. */
    final File catalogFile = new File(filename);
    if (!overwrite && catalogFile.exists()) {
      throw new IOException(filename + " already exists");
    }
    return Catalog.createFromFile(catalogFile, description);
  }

  /**
   * 
   * @param catalogFile a File object pointing to the SQLite database that will store the Catalog. If catalogFile is
   *          null, this creates an in-memory SQLite database.
   * @param description specifies a description for the configuration stored in this Catalog.
   * @return a fresh Catalog fitting the specified description.
   * @throws CatalogException if there is an error opening the database.
   * 
   *           TODO add some sanity checks to the filename?
   */
  private static Catalog createFromFile(final File catalogFile, final String description) throws CatalogException {
    Objects.requireNonNull(description);

    /* Connect to the database. */
    final SQLiteQueue queue = new SQLiteQueue(catalogFile).start();
    try {
      queue.execute(new SQLiteJob<Object>() {

        @Override
        protected Object job(final SQLiteConnection sqliteConnection) throws SQLiteException, CatalogException {
          /* Create all the tables in the Catalog. */
          try {
            sqliteConnection.exec("CREATE TABLE configuration (\n" + "    key STRING UNIQUE NOT NULL,\n"
                + "    value STRING NOT NULL);");
            sqliteConnection.exec("CREATE TABLE workers (\n" + "    worker_id INTEGER PRIMARY KEY ASC,\n"
                + "    host_port STRING NOT NULL);");
            sqliteConnection.exec("CREATE TABLE alive_workers (\n"
                + "    worker_id INTEGER PRIMARY KEY ASC REFERENCES workers(worker_id));");
            sqliteConnection.exec("CREATE TABLE masters (\n" + "    master_id INTEGER PRIMARY KEY ASC,\n"
                + "    host_port STRING NOT NULL);");
            sqliteConnection.exec("CREATE TABLE relations (\n" + "    relation_id INTEGER PRIMARY KEY ASC,\n"
                + "    relation_name STRING NOT NULL UNIQUE);");
            sqliteConnection.exec("CREATE TABLE relation_schema (\n"
                + "    relation_id INTEGER NOT NULL REFERENCES relations(relation_id),\n"
                + "    col_index INTEGER NOT NULL,\n" + "    col_name STRING,\n" + "    col_type STRING NOT NULL);");
            sqliteConnection.exec("CREATE TABLE stored_relations (\n"
                + "    stored_relation_id INTEGER PRIMARY KEY ASC,\n"
                + "    relation_id INTEGER NOT NULL REFERENCES relation_metadata(relation_id),\n"
                + "    num_shards INTEGER NOT NULL,\n" + "    partitioned STRING NOT NULL);");
            sqliteConnection.exec("CREATE TABLE shards (\n"
                + "    stored_relation_id INTEGER NOT NULL REFERENCES stored_relations(stored_relation_id),\n"
                + "    shard_index INTEGER NOT NULL,\n"
                + "    worker_id INTEGER NOT NULL REFERENCES workers(worker_id),\n" + "    location STRING NOT NULL);");
          } catch (final SQLiteException e) {
            LOGGER.error(e.toString());
            throw new CatalogException("SQLiteException while creating new Catalog tables", e);
          }

          /* Populate what tables we can. */
          try {
            final SQLiteStatement statement =
                sqliteConnection.prepare("INSERT INTO configuration (key, value) VALUES (?,?);", false);
            statement.bind(1, "description");
            statement.bind(2, description);
            statement.step();
            statement.dispose();
          } catch (final SQLiteException e) {
            LOGGER.error(e.toString());
            throw new CatalogException("SQLiteException while populating new Catalog tables", e);
          }
          return null;
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }

    return new Catalog(queue);
  }

  /**
   * @param description specifies a description for the configuration stored in this Catalog.
   * @return a fresh Catalog fitting the specified description.
   * @throws CatalogException if there is an error opening the database.
   * 
   *           TODO add some sanity checks to the filename?
   */
  public static Catalog createInMemory(final String description) throws CatalogException {
    Objects.requireNonNull(description);

    return Catalog.createFromFile(null, description);
  }

  /**
   * Opens the Myriad catalog stored as a SQLite database in the specified file.
   * 
   * @param filename the path to the SQLite database storing the catalog.
   * @return an initialized Catalog object ready to be used for experiments.
   * @throws FileNotFoundException if the given file does not exist.
   * @throws CatalogException if there is an error connecting to the database.
   * 
   *           TODO add some sanity checks to the filename?
   */
  public static Catalog open(final String filename) throws FileNotFoundException, CatalogException {
    Objects.requireNonNull(filename);

    /* Ensure the file does actually exist. */
    final File catalogFile = new File(filename);
    if (!catalogFile.exists()) {
      throw new FileNotFoundException(filename);
    }

    /* Connect to the database */
    return new Catalog(new SQLiteQueue(catalogFile).start());
  }

  /**
   * The description of the setup specified by this Catalog. For example, this could be "two node local test" or
   * "20-node Greenplum cluster".
   */
  private String description = null;

  /** Is the Catalog closed? */
  private boolean isClosed = true;

  /** SQLite queue confines all SQLite operations to the same thread. */
  private final SQLiteQueue queue;

  /**
   * Not publicly accessible.
   * 
   * @param queue thread manager for the SQLite database that stores the Catalog.
   * @throws CatalogException if there is an error turning on foreign keys or locking the database.
   */
  private Catalog(final SQLiteQueue queue) throws CatalogException {
    this.queue = queue;
    isClosed = false;
    try {
      queue.execute(new SQLiteJob<Object>() {
        @Override
        protected Object job(final SQLiteConnection sqliteConnection) throws SQLiteException {
          sqliteConnection.exec("PRAGMA foreign_keys = ON;");
          sqliteConnection.exec("PRAGMA locking_mode = EXCLUSIVE;");
          sqliteConnection.exec("BEGIN EXCLUSIVE;");
          sqliteConnection.exec("COMMIT;");
          return null;
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Adds a master using the specified host and port to the Catalog.
   * 
   * @param hostPortString specifies the path to the master in the format "host:port"
   * @return this Catalog
   * @throws CatalogException if the hostPortString is invalid or there is a database exception.
   */
  public Catalog addMaster(final String hostPortString) throws CatalogException {
    Objects.requireNonNull(hostPortString);
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    @SuppressWarnings("unused")
    /* Just used to verify that hostPortString is legal */
    final SocketInfo sockInfo = SocketInfo.valueOf(hostPortString);

    /* Do the work */
    try {
      queue.execute(new SQLiteJob<Object>() {
        @Override
        protected Object job(final SQLiteConnection sqliteConnection) throws SQLiteException {
          final SQLiteStatement statement =
              sqliteConnection.prepare("INSERT INTO masters(host_port) VALUES(?);", false);
          statement.bind(1, hostPortString);
          statement.step();
          statement.dispose();
          return null;
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }

    return this;
  }

  /**
   * Adds the metadata for a relation into the Catalog.
   * 
   * @param name the name of the relation.
   * @param schema the schema of the relation.
   * @throws CatalogException if the relation is already in the catalog or there is an error in the database.
   * 
   *           TODO if we use SQLite in a multithreaded way this will need to be revisited.
   * 
   */
  public void addRelationMetadata(final String name, final Schema schema) throws CatalogException {
    Objects.requireNonNull(name);
    Objects.requireNonNull(schema);
    /* Do the work */
    try {
      queue.execute(new SQLiteJob<Object>() {
        @Override
        protected Object job(final SQLiteConnection sqliteConnection) throws CatalogException, SQLiteException {
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
            for (int i = 0; i < schema.numFields(); ++i) {
              statement.bind(2, i);
              statement.bind(3, schema.getFieldName(i));
              statement.bind(4, schema.getFieldType(i).toString());
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
          return null;
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Adds a worker using the specified host and port to the Catalog.
   * 
   * @param hostPortString specifies the path to the worker in the format "host:port"
   * @return this Catalog
   * @throws CatalogException if the hostPortString is invalid or there is a database exception.
   */
  public Catalog addWorker(final String hostPortString) throws CatalogException {
    Objects.requireNonNull(hostPortString);
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }
    try {
      queue.execute(new SQLiteJob<Object>() {
        @Override
        protected Object job(final SQLiteConnection sqliteConnection) throws SQLiteException, CatalogException {
          try {
            @SuppressWarnings("unused")
            /* Just used to verify that hostPortString is legal */
            final SocketInfo sockInfo = SocketInfo.valueOf(hostPortString);
            final SQLiteStatement statement =
                sqliteConnection.prepare("INSERT INTO workers(host_port) VALUES(?);", false);
            statement.bind(1, hostPortString);
            statement.step();
            statement.dispose();
          } catch (final SQLiteException e) {
            LOGGER.error(e.toString());
            throw new CatalogException(e);
          }
          return null;
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
    return this;
  }

  /**
   * Close the connection to the database that stores the Catalog. Idempotent. Calling any methods (other than close())
   * on this Catalog will throw a CatalogException.
   */
  public void close() {
    try {
      queue.stop(true).join();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (description != null) {
      description = null;
    }
    isClosed = true;
  }

  /**
   * @return the set of workers that are alive.
   * @throws CatalogException if there is an error in the database.
   */
  @SuppressWarnings("unchecked")
  public Set<Integer> getAliveWorkers() throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    try {
      return (Set<Integer>) queue.execute(new SQLiteJob<Object>() {
        @Override
        protected Object job(final SQLiteConnection sqliteConnection) throws SQLiteException, CatalogException {
          final Set<Integer> workers = new HashSet<Integer>();

          try {
            final SQLiteStatement statement = sqliteConnection.prepare("SELECT worker_id FROM alive_workers;", false);
            while (statement.step()) {
              workers.add(statement.columnInt(0));
            }
            statement.dispose();
          } catch (final SQLiteException e) {
            LOGGER.error(e.toString());
            throw new CatalogException(e);
          }

          return workers;
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
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
      throw new CatalogException("Catalog is closed.");
    }
    /* Do the work */
    try {
      return (String) queue.execute(new SQLiteJob<Object>() {
        @Override
        protected Object job(final SQLiteConnection sqliteConnection) throws CatalogException, SQLiteException {
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
            LOGGER.error(e.toString());
            throw new CatalogException(e);
          }
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * @return the description of this Catalog.
   * @throws CatalogException if there is an error extracting the description from the database.
   */
  public String getDescription() throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }
    /* If we have the answer cached, use it. */
    if (description != null) {
      return description;
    }

    description = getConfigurationValue("description");
    return description;
  }

  /**
   * @return the set of masters stored in this Catalog.
   * @throws CatalogException if there is an error in the database.
   */
  @SuppressWarnings("unchecked")
  public List<SocketInfo> getMasters() throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    try {
      return (List<SocketInfo>) queue.execute(new SQLiteJob<Object>() {
        @Override
        protected Object job(final SQLiteConnection sqliteConnection) throws SQLiteException, CatalogException {
          final ArrayList<SocketInfo> masters = new ArrayList<SocketInfo>();
          try {
            final SQLiteStatement statement = sqliteConnection.prepare("SELECT * FROM masters;", false);
            while (statement.step()) {
              masters.add(SocketInfo.valueOf(statement.columnString(1)));
            }
            statement.dispose();
          } catch (final SQLiteException e) {
            LOGGER.error(e.toString());
            throw new CatalogException(e);
          }

          return masters;
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * @return the set of workers stored in this Catalog.
   * @throws CatalogException if there is an error in the database.
   */
  @SuppressWarnings("unchecked")
  public Map<Integer, SocketInfo> getWorkers() throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    try {
      return (Map<Integer, SocketInfo>) queue.execute(new SQLiteJob<Object>() {
        @Override
        protected Object job(final SQLiteConnection sqliteConnection) throws SQLiteException, CatalogException {
          final Map<Integer, SocketInfo> workers = new HashMap<Integer, SocketInfo>();

          try {
            final SQLiteStatement statement = sqliteConnection.prepare("SELECT * FROM workers;", false);
            while (statement.step()) {
              workers.put(statement.columnInt(0), SocketInfo.valueOf(statement.columnString(1)));
            }
            statement.dispose();
          } catch (final SQLiteException e) {
            LOGGER.error(e.toString());
            throw new CatalogException(e);
          }

          return workers;
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }
}
