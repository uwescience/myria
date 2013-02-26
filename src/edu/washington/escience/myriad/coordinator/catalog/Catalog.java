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
import java.util.logging.Level;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteJob;
import com.almworks.sqlite4java.SQLiteQueue;
import com.almworks.sqlite4java.SQLiteStatement;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.Schema;
import edu.washington.escience.myriad.Type;
import edu.washington.escience.myriad.parallel.SocketInfo;

/**
 * This class is intended to store the configuration information for a Myriad installation.
 * 
 * @author dhalperi
 * 
 */
public final class Catalog {
  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(Catalog.class.getName());

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
            sqliteConnection.exec("CREATE TABLE relations (\n" + "    user_name STRING NOT NULL,\n"
                + "    program_name STRING NOT NULL,\n" + "    relation_name STRING NOT NULL,\n"
                + "    PRIMARY KEY (user_name,program_name,relation_name));");
            sqliteConnection.exec("CREATE TABLE relation_schema (\n" + "    user_name STRING NOT NULL,\n"
                + "    program_name STRING NOT NULL,\n" + "    relation_name STRING NOT NULL,\n"
                + "    col_index INTEGER NOT NULL,\n" + "    col_name STRING,\n" + "    col_type STRING NOT NULL,\n"
                + "    FOREIGN KEY (user_name,program_name,relation_name) REFERENCES relations);");
            sqliteConnection.exec("CREATE TABLE stored_relations (\n"
                + "    stored_relation_id INTEGER PRIMARY KEY ASC,\n" + "    user_name STRING NOT NULL,\n"
                + "    program_name STRING NOT NULL,\n" + "    relation_name STRING NOT NULL,\n"
                + "    num_shards INTEGER NOT NULL,\n" + "    how_partitioned STRING NOT NULL,\n"
                + "    FOREIGN KEY (user_name,program_name,relation_name) REFERENCES relations);");
            sqliteConnection.exec("CREATE TABLE shards (\n"
                + "    stored_relation_id INTEGER NOT NULL REFERENCES stored_relations(stored_relation_id),\n"
                + "    shard_index INTEGER NOT NULL,\n"
                + "    worker_id INTEGER NOT NULL REFERENCES workers(worker_id));");
            sqliteConnection.exec("CREATE TABLE queries (\n" + "    query_id INTEGER NOT NULL PRIMARY KEY ASC,\n"
                + "    raw_query TEXT NOT NULL,\n" + "    logical_ra TEXT NOT NULL);");
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

    java.util.logging.Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    java.util.logging.Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

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
   * @return the list of known relations in the Catalog.
   * @throws CatalogException if the relation is already in the catalog or there is an error in the database.
   */
  public List<RelationKey> getRelations() throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }
    try {
      return queue.execute(new SQLiteJob<List<RelationKey>>() {
        @Override
        protected List<RelationKey> job(final SQLiteConnection sqliteConnection) throws SQLiteException,
            CatalogException {
          final List<RelationKey> relations = new ArrayList<RelationKey>();

          try {
            final SQLiteStatement statement =
                sqliteConnection.prepare("SELECT user_name,program_name,relation_name FROM relations;", false);
            while (statement.step()) {
              relations.add(RelationKey.of(statement.columnString(0), statement.columnString(1), statement
                  .columnString(2)));
            }
            statement.dispose();
          } catch (final SQLiteException e) {
            LOGGER.error(e.toString());
            throw new CatalogException(e);
          }

          return relations;
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Adds the metadata for a relation into the Catalog.
   * 
   * @param relation the relation to create.
   * @param schema the schema of the relation.
   * @throws CatalogException if the relation is already in the catalog or there is an error in the database.
   */
  public void addRelationMetadata(final RelationKey relation, final Schema schema) throws CatalogException {
    Objects.requireNonNull(relation);
    Objects.requireNonNull(schema);
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    /* Do the work */
    try {
      queue.execute(new SQLiteJob<Object>() {
        @Override
        protected Object job(final SQLiteConnection sqliteConnection) throws CatalogException, SQLiteException {
          try {
            /* To begin: start a transaction. */
            sqliteConnection.exec("BEGIN TRANSACTION;");

            /* First, insert the relation name. */
            SQLiteStatement statement =
                sqliteConnection
                    .prepare("INSERT INTO relations (user_name,program_name,relation_name) VALUES (?,?,?);");
            statement.bind(1, relation.getUserName());
            statement.bind(2, relation.getProgramName());
            statement.bind(3, relation.getRelationName());
            statement.stepThrough();
            statement.dispose();
            statement = null;

            /* Third, populate the Schema table. */
            statement =
                sqliteConnection
                    .prepare("INSERT INTO relation_schema(user_name,program_name,relation_name,col_index,col_name,col_type) "
                        + "VALUES (?,?,?,?,?,?);");
            statement.bind(1, relation.getUserName());
            statement.bind(2, relation.getProgramName());
            statement.bind(3, relation.getRelationName());
            for (int i = 0; i < schema.numColumns(); ++i) {
              statement.bind(4, i);
              statement.bind(5, schema.getColumnName(i));
              statement.bind(6, schema.getColumnType(i).toString());
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
   * Adds the metadata for a relation into the Catalog.
   * 
   * @param relation the relation to create.
   * @param workers the IDs of the workers storing this copy of the relation.
   * @param howPartitioned how this copy of the relation is partitioned.
   * @throws CatalogException if there is an error in the database.
   */
  public void addStoredRelation(final RelationKey relation, final Set<Integer> workers, final String howPartitioned)
      throws CatalogException {
    Objects.requireNonNull(relation);
    Objects.requireNonNull(workers);
    Objects.requireNonNull(howPartitioned);
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    /* Do the work */
    try {
      queue.execute(new SQLiteJob<Object>() {
        @Override
        protected Object job(final SQLiteConnection sqliteConnection) throws CatalogException, SQLiteException {
          try {
            /* To begin: start a transaction. */
            sqliteConnection.exec("BEGIN TRANSACTION;");

            /* First, populate the stored_relation table. */
            SQLiteStatement statement =
                sqliteConnection
                    .prepare("INSERT INTO stored_relations (user_name,program_name,relation_name,num_shards,how_partitioned) VALUES (?,?,?,?,?);");
            statement.bind(1, relation.getUserName());
            statement.bind(2, relation.getProgramName());
            statement.bind(3, relation.getRelationName());
            statement.bind(4, workers.size());
            statement.bind(5, howPartitioned);
            statement.stepThrough();
            statement.dispose();
            statement = null;

            Long storedRelationId = sqliteConnection.getLastInsertId();
            /* Second, populate the shards table. */
            statement =
                sqliteConnection.prepare("INSERT INTO shards(stored_relation_id,shard_index,worker_id) "
                    + "VALUES (?,?,?);");
            statement.bind(1, storedRelationId);
            int count = 0;
            for (int i : workers) {
              statement.bind(2, count);
              statement.bind(3, i);
              statement.step();
              statement.reset(false);
              ++count;
            }
            statement.dispose();
            statement = null;

            /* To complete: commit the transaction. */
            sqliteConnection.exec("COMMIT TRANSACTION;");
          } catch (final SQLiteException e) {
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
  public Set<Integer> getAliveWorkers() throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    try {
      return queue.execute(new SQLiteJob<Set<Integer>>() {
        @Override
        protected Set<Integer> job(final SQLiteConnection sqliteConnection) throws SQLiteException, CatalogException {
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
      return queue.execute(new SQLiteJob<String>() {
        @Override
        protected String job(final SQLiteConnection sqliteConnection) throws CatalogException, SQLiteException {
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
  public List<SocketInfo> getMasters() throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    try {
      return queue.execute(new SQLiteJob<List<SocketInfo>>() {
        @Override
        protected List<SocketInfo> job(final SQLiteConnection sqliteConnection) throws SQLiteException,
            CatalogException {
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
  public Map<Integer, SocketInfo> getWorkers() throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    try {
      return queue.execute(new SQLiteJob<Map<Integer, SocketInfo>>() {
        @Override
        protected Map<Integer, SocketInfo> job(final SQLiteConnection sqliteConnection) throws SQLiteException,
            CatalogException {
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

  /**
   * @param relationKey the key of the desired relation.
   * @return the schema of the specified relation, or null if not found.
   * @throws CatalogException if there is an error accessing the desired Schema.
   */
  public Schema getSchema(final RelationKey relationKey) throws CatalogException {
    Objects.requireNonNull(relationKey);
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    /* Do the work */
    try {
      return queue.execute(new SQLiteJob<Schema>() {
        @Override
        protected Schema job(final SQLiteConnection sqliteConnection) throws CatalogException, SQLiteException {
          try {
            /* First, insert the relation name. */
            SQLiteStatement statement =
                sqliteConnection
                    .prepare("SELECT col_name,col_type FROM relation_schema WHERE user_name=? AND program_name=? AND relation_name=?; ORDER BY col_index ASC");
            statement.bind(1, relationKey.getUserName());
            statement.bind(2, relationKey.getProgramName());
            statement.bind(3, relationKey.getRelationName());
            ImmutableList.Builder<String> names = ImmutableList.builder();
            ImmutableList.Builder<Type> types = ImmutableList.builder();
            if (!statement.step()) {
              return null;
            }
            do {
              names.add(statement.columnString(0));
              types.add(Type.valueOf(statement.columnString(1)));
            } while (statement.step());
            statement.dispose();
            return new Schema(types, names);
          } catch (final SQLiteException e) {
            throw new CatalogException(e);
          }
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Insert a new query into the Catalog.
   * 
   * @param rawQuery the original user data of the query.
   * @param logicalRa the compiled logical relational algebra plan of the query.
   * @return the newly generated ID of this query.
   * @throws CatalogException if there is an error adding the new query.
   */
  public Long newQuery(final String rawQuery, final String logicalRa) throws CatalogException {
    Objects.requireNonNull(rawQuery);
    Objects.requireNonNull(logicalRa);
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    try {
      return queue.execute(new SQLiteJob<Long>() {
        @Override
        protected Long job(final SQLiteConnection sqliteConnection) throws CatalogException, SQLiteException {
          try {
            /* First, insert the relation name. */
            SQLiteStatement statement =
                sqliteConnection.prepare("INSERT INTO queries (raw_query,logical_ra) VALUES (?,?);");
            statement.bind(1, rawQuery);
            statement.bind(2, logicalRa);
            statement.stepThrough();
            statement.dispose();
            return sqliteConnection.getLastInsertId();
          } catch (final SQLiteException e) {
            throw new CatalogException(e);
          }
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }
}
