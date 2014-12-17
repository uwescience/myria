package edu.washington.escience.myria.coordinator.catalog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

import javax.annotation.Nullable;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteJob;
import com.almworks.sqlite4java.SQLiteQueue;
import com.almworks.sqlite4java.SQLiteStatement;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import edu.washington.escience.myria.MyriaConstants.FTMode;
import edu.washington.escience.myria.MyriaConstants.ProfilingMode;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.api.MyriaJsonMapperProvider;
import edu.washington.escience.myria.api.encoding.DatasetStatus;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.api.encoding.plan.SubPlanEncoding;
import edu.washington.escience.myria.parallel.Query;
import edu.washington.escience.myria.parallel.SocketInfo;

/**
 * This class is intended to store the configuration information for a Myria installation.
 * 
 * 
 */
public final class MasterCatalog {
  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(MasterCatalog.class);

  /** CREATE TABLE statements @formatter:off */
  /** Create the configurations table. */
  private static final String CREATE_CONFIGURATION =
      "CREATE TABLE configuration (\n"
    + "    key TEXT UNIQUE NOT NULL,\n"
    + "    value TEXT NOT NULL);";
  /** Create the masters table. */
  private static final String CREATE_MASTERS =
      "CREATE TABLE masters (\n"
    + "    master_id INTEGER PRIMARY KEY ASC,\n"
    + "    host_port TEXT NOT NULL);";
  /** Create the workers table. */
  private static final String CREATE_WORKERS =
      "CREATE TABLE workers (\n"
    + "    worker_id INTEGER PRIMARY KEY ASC,\n"
    + "    host_port TEXT NOT NULL);";
  /** Create the alive_workers table. */
  private static final String CREATE_ALIVE_WORKERS =
      "CREATE TABLE alive_workers (\n"
    + "    worker_id INTEGER PRIMARY KEY ASC REFERENCES workers);";
  /** Create the queries table. */
  private static final String CREATE_QUERIES =
      "CREATE TABLE queries (\n"
    + "    query_id INTEGER NOT NULL PRIMARY KEY ASC,\n"
    + "    raw_query TEXT NOT NULL,\n"
    + "    logical_ra TEXT NOT NULL,\n"
    + "    plan TEXT NOT NULL,\n"
    + "    submit_time TEXT NOT NULL, -- DATES IN ISO8601 FORMAT \n"
    + "    start_time TEXT, -- DATES IN ISO8601 FORMAT \n"
    + "    finish_time TEXT, -- DATES IN ISO8601 FORMAT \n"
    + "    elapsed_nanos INTEGER,\n"
    + "    status TEXT NOT NULL,\n"
    + "    message TEXT,\n"
    + "    profiling_mode TEXT,\n" 
    + "    ft_mode TEXT,\n"
    + "    language TEXT);";
  /** Create the relations table. */
  private static final String CREATE_RELATIONS =
      "CREATE TABLE relations (\n"
    + "    user_name TEXT NOT NULL,\n"
    + "    program_name TEXT NOT NULL,\n"
    + "    relation_name TEXT NOT NULL,\n"
    + "    num_tuples INTEGER NOT NULL,\n"
    + "    query_id INTEGER NOT NULL REFERENCES queries(query_id),\n"
    + "    PRIMARY KEY (user_name,program_name,relation_name));";
  /** Create the relation_schema table. */
  private static final String CREATE_RELATION_SCHEMA =
      "CREATE TABLE relation_schema (\n"
    + "    user_name TEXT NOT NULL,\n"
    + "    program_name TEXT NOT NULL,\n"
    + "    relation_name TEXT NOT NULL,\n"
    + "    col_index INTEGER NOT NULL,\n"
    + "    col_name TEXT,\n"
    + "    col_type TEXT NOT NULL,\n"
    + "    FOREIGN KEY (user_name,program_name,relation_name) REFERENCES relations ON DELETE CASCADE);";
  /** Create the stored_relations table. */
  private static final String CREATE_STORED_RELATIONS =
      "CREATE TABLE stored_relations (\n"
    + "    stored_relation_id INTEGER PRIMARY KEY ASC,\n"
    + "    user_name TEXT NOT NULL,\n"
    + "    program_name TEXT NOT NULL,\n"
    + "    relation_name TEXT NOT NULL,\n"
    + "    num_shards INTEGER NOT NULL,\n"
    + "    how_partitioned TEXT NOT NULL,\n"
    + "    FOREIGN KEY (user_name,program_name,relation_name) REFERENCES relations ON DELETE CASCADE);";
  /** Create the stored_relations table. */
  private static final String CREATE_SHARDS =
      "CREATE TABLE shards (\n"
    + "    stored_relation_id INTEGER NOT NULL REFERENCES stored_relations ON DELETE CASCADE,\n"
    + "    shard_index INTEGER NOT NULL,\n"
    + "    worker_id INTEGER NOT NULL REFERENCES workers);";
  /** Create the stored_relations table. */
  private static final String UPDATE_UNKNOWN_STATUS =
      "UPDATE queries "
    + "SET status = '" + QueryStatusEncoding.Status.UNKNOWN.toString() + "'"
    + "WHERE status = '" + QueryStatusEncoding.Status.ACCEPTED.toString() + "';";
 /** CREATE TABLE statements @formatter:on */

  /**
   * @param filename the path to the SQLite database storing the catalog.
   * @return a fresh Catalog fitting the specified description.
   * @throws IOException if the specified file already exists.
   * @throws CatalogException if there is an error opening the database.
   * 
   *           TODO add some sanity checks to the filename?
   */
  public static MasterCatalog create(final String filename) throws IOException, CatalogException {
    Objects.requireNonNull(filename);
    return MasterCatalog.create(filename, false);
  }

  /**
   * @param filename the path to the SQLite database storing the catalog.
   * @param overwrite specifies whether to overwrite an existing Catalog.
   * @return a fresh Catalog fitting the specified description.
   * @throws IOException if overwrite is true and the specified file already exists.
   * @throws CatalogException if there is an error opening the database.
   * 
   *           TODO add some sanity checks to the filename?
   */
  public static MasterCatalog create(final String filename, final boolean overwrite) throws IOException,
      CatalogException {
    Objects.requireNonNull(filename);

    /* if overwrite is false, error if the file exists. */
    final File catalogFile = new File(filename);
    if (!overwrite && catalogFile.exists()) {
      throw new IOException(filename + " already exists");
    }
    return MasterCatalog.createFromFile(catalogFile);
  }

  /**
   * 
   * @param catalogFile a File object pointing to the SQLite database that will store the Catalog. If catalogFile is
   *          null, this creates an in-memory SQLite database.
   * @return a fresh Catalog fitting the specified description.
   * @throws CatalogException if there is an error opening the database.
   * 
   *           TODO add some sanity checks to the filename?
   */
  private static MasterCatalog createFromFile(final File catalogFile) throws CatalogException {
    /* Connect to the database. */
    final SQLiteQueue queue = new SQLiteQueue(catalogFile).start();
    try {
      queue.execute(new SQLiteJob<Object>() {

        @Override
        protected Object job(final SQLiteConnection sqliteConnection) throws SQLiteException, CatalogException {
          /* Create all the tables in the Catalog. */
          try {
            sqliteConnection.exec("PRAGMA journal_mode = WAL;");
            sqliteConnection.exec("BEGIN TRANSACTION");
            sqliteConnection.exec(CREATE_CONFIGURATION);
            sqliteConnection.exec(CREATE_MASTERS);
            sqliteConnection.exec(CREATE_WORKERS);
            sqliteConnection.exec(CREATE_ALIVE_WORKERS);
            sqliteConnection.exec(CREATE_QUERIES);
            sqliteConnection.exec(CREATE_RELATIONS);
            sqliteConnection.exec(CREATE_RELATION_SCHEMA);
            sqliteConnection.exec(CREATE_STORED_RELATIONS);
            sqliteConnection.exec(CREATE_SHARDS);
            sqliteConnection.exec("END TRANSACTION");
          } catch (final SQLiteException e) {
            sqliteConnection.exec("ROLLBACK TRANSACTION");
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error(e.toString());
            }
            throw new CatalogException("SQLiteException while creating new Catalog tables", e);
          }
          return null;
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }

    return new MasterCatalog(queue);
  }

  /**
   * @return a fresh Catalog fitting the specified description.
   * @throws CatalogException if there is an error opening the database.
   */
  public static MasterCatalog createInMemory() throws CatalogException {
    return MasterCatalog.createFromFile(null);
  }

  /**
   * Opens the Myria catalog stored as a SQLite database in the specified file.
   * 
   * @param filename the path to the SQLite database storing the catalog.
   * @return an initialized Catalog object ready to be used for experiments.
   * @throws FileNotFoundException if the given file does not exist.
   * @throws CatalogException if there is an error connecting to the database.
   * 
   *           TODO add some sanity checks to the filename?
   */
  public static MasterCatalog open(final String filename) throws FileNotFoundException, CatalogException {
    Objects.requireNonNull(filename, "filename");

    java.util.logging.Logger.getLogger("com.almworks.sqlite4java").setLevel(Level.SEVERE);
    java.util.logging.Logger.getLogger("com.almworks.sqlite4java.Internal").setLevel(Level.SEVERE);

    /* Ensure the file does actually exist. */
    final File catalogFile = new File(filename);
    if (!catalogFile.exists()) {
      throw new FileNotFoundException(filename);
    }

    /* Connect to the database */
    return new MasterCatalog(new SQLiteQueue(catalogFile).start());
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
  private MasterCatalog(final SQLiteQueue queue) throws CatalogException {
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
          sqliteConnection.exec(UPDATE_UNKNOWN_STATUS);
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
  public MasterCatalog addMaster(final String hostPortString) throws CatalogException {
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
   * @param numTuples the number of tuples in the relation.
   * @param queryId the query that created the relation.
   * @throws CatalogException if the relation is already in the catalog or there is an error in the database.
   */
  public void addRelationMetadata(final RelationKey relation, final Schema schema, final long numTuples,
      final long queryId) throws CatalogException {
    Objects.requireNonNull(relation, "relation");
    Objects.requireNonNull(schema, "schema");
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
                    .prepare("INSERT INTO relations (user_name,program_name,relation_name,num_tuples,query_id) VALUES (?,?,?,?,?);");
            statement.bind(1, relation.getUserName());
            statement.bind(2, relation.getProgramName());
            statement.bind(3, relation.getRelationName());
            statement.bind(4, numTuples);
            statement.bind(5, queryId);
            statement.stepThrough();
            statement.dispose();
            statement = null;

            /* Second, populate the Schema table. */
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
              sqliteConnection.exec("ROLLBACK TRANSACTION;");
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
   * @param workerId the worker id to be added
   * @return this Catalog
   * @throws CatalogException if the hostPortString is invalid or there is a database exception.
   */
  public MasterCatalog addWorker(final int workerId, final String hostPortString) throws CatalogException {
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
          return null;
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
    return this;
  }

  /**
   * Add workers.
   * 
   * @param workers workerId -> "host:port"
   * @return this Catalog
   * @throws CatalogException if the hostPortString is invalid or there is a database exception.
   */
  public MasterCatalog addWorkers(final Map<String, String> workers) throws CatalogException {
    Objects.requireNonNull(workers);
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }
    try {
      queue.execute(new SQLiteJob<Object>() {
        @Override
        protected Object job(final SQLiteConnection sqliteConnection) throws SQLiteException, CatalogException {
          try {
            final SQLiteStatement statement =
                sqliteConnection.prepare("INSERT INTO workers(worker_id, host_port) VALUES(?,?);", false);
            /* To begin: start a transaction. */
            sqliteConnection.exec("BEGIN TRANSACTION;");

            for (final Map.Entry<String, String> e : workers.entrySet()) {
              @SuppressWarnings("unused")
              /* Just used to verify that hostPortString is legal */
              final SocketInfo sockInfo = SocketInfo.valueOf(e.getValue());
              statement.bind(1, Integer.valueOf(e.getKey()));
              statement.bind(2, e.getValue());
              statement.step();
              statement.reset(false);
            }
            /* To complete: commit the transaction. */
            sqliteConnection.exec("COMMIT TRANSACTION;");
            statement.dispose();
          } catch (final SQLiteException e) {
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error(e.toString());
            }
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
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error(e.toString());
            }
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
   * @return All the configuration values.
   * @throws CatalogException if error occurs in catalog parsing.
   */
  public ImmutableMap<String, String> getAllConfigurations() throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }
    try {
      return queue.execute(new SQLiteJob<ImmutableMap<String, String>>() {
        @Override
        protected ImmutableMap<String, String> job(final SQLiteConnection sqliteConnection) throws CatalogException,
            SQLiteException {
          ImmutableMap.Builder<String, String> configurations = ImmutableMap.builder();
          try {

            final SQLiteStatement statement = sqliteConnection.prepare("SELECT * FROM configuration;", false);
            while (statement.step()) {
              configurations.put(statement.columnString(0), statement.columnString(1));
            }
            statement.dispose();
            return configurations.build();
          } catch (final SQLiteException e) {
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error(e.toString());
            }
            throw new CatalogException(e);
          }
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
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error(e.toString());
            }
            throw new CatalogException(e);
          }
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
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
      queue.execute(new SQLiteJob<Object>() {
        @Override
        protected Object job(final SQLiteConnection sqliteConnection) throws CatalogException, SQLiteException {
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
            return null;
          } catch (final SQLiteException e) {
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error(e.toString());
            }
            /* Commit transaction. */
            sqliteConnection.exec("ROLLBACK TRANSACTION");
            throw new CatalogException(e);
          }
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
   * @param value the value of the configuration parameter.
   * @throws CatalogException if there is an error in the backing database.
   */
  public void setConfigurationValue(final String key, final String value) throws CatalogException {
    Objects.requireNonNull(key);
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }
    try {
      queue.execute(new SQLiteJob<String>() {
        @Override
        protected String job(final SQLiteConnection sqliteConnection) throws CatalogException, SQLiteException {
          try {
            /* Getting this out is a simple query, which does not need to be cached. */
            final SQLiteStatement statement =
                sqliteConnection.prepare("INSERT INTO configuration VALUES(?,?);", false).bind(1, key).bind(2, value);
            if (!statement.step()) {
              /* If step() returns false, there's no data. Return null. */
              return null;
            }
            statement.dispose();
            return null;
          } catch (final SQLiteException e) {
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error(e.toString());
            }
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
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error(e.toString());
            }
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
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error(e.toString());
            }
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
   * @return A list of datasets in the system.
   * @throws CatalogException if there is an error accessing the desired Schema.
   */
  public List<DatasetStatus> getDatasets() throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    /* Do the work */
    try {
      return queue.execute(new SQLiteJob<List<DatasetStatus>>() {
        @Override
        protected List<DatasetStatus> job(final SQLiteConnection sqliteConnection) throws CatalogException,
            SQLiteException {
          try {
            SQLiteStatement statement =
                sqliteConnection
                    .prepare("SELECT user_name, program_name, relation_name, num_tuples, query_id, finish_time FROM relations JOIN queries USING (query_id) ORDER BY user_name, program_name, relation_name ASC");
            return datasetStatusListHelper(statement, sqliteConnection);
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
   * @param userName a user.
   * @return A list of datasets owned by the specified user.
   * @throws CatalogException if there is an error accessing the desired Schema.
   */
  public List<DatasetStatus> getDatasetsForUser(final String userName) throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    /* Do the work */
    try {
      return queue.execute(new SQLiteJob<List<DatasetStatus>>() {
        @Override
        protected List<DatasetStatus> job(final SQLiteConnection sqliteConnection) throws CatalogException,
            SQLiteException {
          try {
            SQLiteStatement statement =
                sqliteConnection
                    .prepare("SELECT user_name, program_name, relation_name, num_tuples, query_id, finish_time FROM relations JOIN queries USING (query_id) WHERE user_name=? ORDER BY user_name, program_name, relation_name ASC");
            statement.bind(1, userName);
            return datasetStatusListHelper(statement, sqliteConnection);
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
   * @param userName a user.
   * @param programName a program by the specified user.
   * @return A list of datasets belonging to the specified program.
   * @throws CatalogException if there is an error accessing the desired Schema.
   */
  public List<DatasetStatus> getDatasetsForProgram(final String userName, final String programName)
      throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    /* Do the work */
    try {
      return queue.execute(new SQLiteJob<List<DatasetStatus>>() {
        @Override
        protected List<DatasetStatus> job(final SQLiteConnection sqliteConnection) throws CatalogException,
            SQLiteException {
          try {
            SQLiteStatement statement =
                sqliteConnection
                    .prepare("SELECT user_name, program_name, relation_name, num_tuples, query_id, finish_time FROM relations JOIN queries USING (query_id) WHERE user_name=? AND program_name=? ORDER BY user_name, program_name, relation_name ASC");
            statement.bind(1, userName);
            statement.bind(2, programName);
            return datasetStatusListHelper(statement, sqliteConnection);
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
   * @param queryId the id of the query.
   * @return A list of datasets belonging to the specified program.
   * @throws CatalogException if there is an error accessing the desired Schema.
   */
  public List<DatasetStatus> getDatasetsForQuery(final int queryId) throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    /* Do the work */
    try {
      return queue.execute(new SQLiteJob<List<DatasetStatus>>() {
        @Override
        protected List<DatasetStatus> job(final SQLiteConnection sqliteConnection) throws CatalogException,
            SQLiteException {
          try {
            SQLiteStatement statement =
                sqliteConnection
                    .prepare("SELECT user_name, program_name, relation_name, num_tuples, query_id, finish_time FROM relations JOIN queries USING (query_id) WHERE query_id=? ORDER BY user_name, program_name, relation_name ASC");
            statement.bind(1, queryId);
            return datasetStatusListHelper(statement, sqliteConnection);
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
   * Fetch the schema for the specified dataset, or null if the dataset is not found.
   * 
   * @param statement a cursor over the relations table of the relation status to be generated.
   * @param connection a connection to the catalog, used in nested queries.
   * @return a List<DatasetStatus>, one for each relation in the sqliteStatement.
   * @throws CatalogException if there is an error in the catalog.
   */
  private static List<DatasetStatus> datasetStatusListHelper(final SQLiteStatement statement,
      final SQLiteConnection connection) throws CatalogException {
    try {
      ImmutableList.Builder<DatasetStatus> result = ImmutableList.builder();
      while (statement.step()) {
        RelationKey relationKey =
            RelationKey.of(statement.columnString(0), statement.columnString(1), statement.columnString(2));
        long numTuples = statement.columnLong(3);
        long queryId = statement.columnLong(4);
        String created = statement.columnString(5);
        result.add(new DatasetStatus(relationKey, getDatasetSchema(connection, relationKey), numTuples, queryId,
            created));
      }
      statement.dispose();
      return result.build();
    } catch (final SQLiteException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Fetch the schema for the specified dataset, or null if the dataset is not found.
   * 
   * @param sqliteConnection a connection to the catalog.
   * @param relationKey the key specifying the target relation.
   * @return the schema for the specified dataset.
   * @throws CatalogException if there is an error in the catalog.
   */
  private static Schema getDatasetSchema(final SQLiteConnection sqliteConnection, final RelationKey relationKey)
      throws CatalogException {
    try {
      SQLiteStatement statement =
          sqliteConnection
              .prepare("SELECT col_name, col_type FROM relation_schema WHERE user_name=? AND program_name=? AND relation_name=? ORDER BY col_index ASC");
      statement.bind(1, relationKey.getUserName());
      statement.bind(2, relationKey.getProgramName());
      statement.bind(3, relationKey.getRelationName());
      if (!statement.step()) {
        return null;
      }
      ImmutableList.Builder<String> columnNames = ImmutableList.builder();
      ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
      while (statement.hasRow()) {
        columnNames.add(statement.columnString(0));
        columnTypes.add(Type.valueOf(statement.columnString(1)));
        statement.step();
      }
      Schema schema = new Schema(columnTypes, columnNames);
      statement.dispose();
      return schema;
    } catch (final SQLiteException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Insert a new query into the Catalog.
   * 
   * @param query the query encoding.
   * @return the newly generated ID of this query.
   * @throws CatalogException if there is an error adding the new query.
   */
  @SuppressWarnings("checkstyle:magicnumber")
  public Long newQuery(final QueryEncoding query) throws CatalogException {
    Objects.requireNonNull(query, "query");
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    final QueryStatusEncoding queryStatus = QueryStatusEncoding.submitted(query);
    final String physicalString;
    try {
      physicalString = MyriaJsonMapperProvider.getMapper().writeValueAsString(query.plan);
    } catch (JsonProcessingException e) {
      throw new CatalogException(e);
    }

    try {
      return queue.execute(new SQLiteJob<Long>() {
        @Override
        protected Long job(final SQLiteConnection sqliteConnection) throws CatalogException, SQLiteException {
          try {
            SQLiteStatement statement =
                sqliteConnection
                    .prepare("INSERT INTO queries (raw_query, logical_ra, plan, submit_time, start_time, finish_time, elapsed_nanos, status, profiling_mode, ft_mode, language) VALUES (?,?,?,?,?,?,?,?,?,?,?);");
            statement.bind(1, queryStatus.rawQuery);
            statement.bind(2, queryStatus.logicalRa);
            statement.bind(3, physicalString);
            statement.bind(4, toStringOrNull(queryStatus.submitTime));
            statement.bind(5, toStringOrNull(queryStatus.startTime));
            statement.bind(6, toStringOrNull(queryStatus.finishTime));
            if (queryStatus.elapsedNanos != null) {
              statement.bind(7, queryStatus.elapsedNanos);
            } else {
              /* Auto-unboxed values must be manually nulled. */
              statement.bindNull(7);
            }
            statement.bind(8, queryStatus.status.toString());
            String modes = "";
            for (ProfilingMode mode : queryStatus.profilingMode) {
              if (!modes.equals("")) {
                modes += ",";
              }
              modes += mode.toString();
            }
            statement.bind(9, modes);
            statement.bind(10, queryStatus.ftMode.toString());
            statement.bind(11, queryStatus.language);
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

  /**
   * Get the status of a query from the MasterCatalog.
   * 
   * @param queryId the ID of the query being retrieved.
   * @return the status of the query.
   * @throws CatalogException if there is an error in the MasterCatalog.
   */
  public QueryStatusEncoding getQuery(final Long queryId) throws CatalogException {
    Objects.requireNonNull(queryId);
    if (isClosed) {
      throw new CatalogException("MasterCatalog is closed.");
    }

    try {
      return queue.execute(new SQLiteJob<QueryStatusEncoding>() {
        @Override
        protected QueryStatusEncoding job(final SQLiteConnection sqliteConnection) throws CatalogException,
            SQLiteException {
          try {
            SQLiteStatement statement =
                sqliteConnection
                    .prepare("SELECT query_id,raw_query,logical_ra,plan,submit_time,start_time,finish_time,elapsed_nanos,status,message,profiling_mode,ft_mode,language FROM queries WHERE query_id=?;");
            statement.bind(1, queryId);
            statement.step();
            if (!statement.hasRow()) {
              return null;
            }
            final QueryStatusEncoding queryStatus = queryStatusHelper(statement);
            statement.dispose();
            return queryStatus;
          } catch (final SQLiteException | IOException e) {
            throw new CatalogException(e);
          }
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Helper function to get a {@link QueryStatusEncoding} from a query over the <code>queries</code> table. This version
   * expects a "simple" query status, which does not contain the logical or physical query plan.
   * 
   * @param statement the query over the <code>queries</code> table. Has been stepped once.
   * @return the status of the first query in the result.
   * @throws SQLiteException if there is an error in the database.
   */
  private static QueryStatusEncoding querySimpleStatusHelper(final SQLiteStatement statement) throws SQLiteException {
    final QueryStatusEncoding queryStatus = new QueryStatusEncoding(statement.columnLong(0));
    queryStatus.rawQuery = statement.columnString(1);
    queryStatus.submitTime = parseDateTime(statement.columnString(2));
    queryStatus.startTime = parseDateTime(statement.columnString(3));
    queryStatus.finishTime = parseDateTime(statement.columnString(4));
    if (!statement.columnNull(5)) {
      queryStatus.elapsedNanos = statement.columnLong(5);
    }
    queryStatus.status = QueryStatusEncoding.Status.valueOf(statement.columnString(6));
    queryStatus.message = statement.columnString(7);
    List<ProfilingMode> modes = new ArrayList<ProfilingMode>();
    for (String mode : statement.columnString(8).split(",")) {
      if (!mode.equals("")) {
        modes.add(ProfilingMode.valueOf(mode));
      }
    }
    queryStatus.profilingMode = ImmutableList.copyOf(modes);
    return queryStatus;
  }

  /**
   * A wrapper for {@link DateTime.parse} that returns null if the string is null.
   * 
   * @param dateTime a string in ISO8601 datetime format.
   * @return the parsed DateTime, or null if the parameter is null.
   */
  private static DateTime parseDateTime(@Nullable final String dateTime) {
    if (dateTime == null) {
      return null;
    }
    return DateTime.parse(dateTime);
  }

  /**
   * A wrapper for {@link Object#toString()} that returns null if the Object is null.
   * 
   * @param o an object.
   * @return o.toString(), or null if o is null.
   */
  private static String toStringOrNull(@Nullable final Object o) {
    if (o == null) {
      return null;
    }
    return o.toString();
  }

  /**
   * Helper function to get a {@link QueryStatusEncoding} from a query over the <code>queries</code> table..
   * 
   * @param statement the query over the <code>queries</code> table. Has been stepped once.
   * @return the status of the first query in the result.
   * @throws SQLiteException if there is an error in the database.
   * @throws IOException if there is an error when deserializing physical plan.
   */
  @SuppressWarnings("checkstyle:magicnumber")
  private static QueryStatusEncoding queryStatusHelper(final SQLiteStatement statement) throws SQLiteException,
      IOException {
    final QueryStatusEncoding queryStatus = new QueryStatusEncoding(statement.columnLong(0));
    queryStatus.rawQuery = statement.columnString(1);
    queryStatus.logicalRa = statement.columnString(2);
    String physicalString = statement.columnString(3);

    try {
      queryStatus.plan = MyriaJsonMapperProvider.getMapper().readValue(physicalString, SubPlanEncoding.class);
    } catch (final IOException e) {
      LOGGER.warn("Error deserializing plan for query #{}", queryStatus.queryId, e);
      queryStatus.plan = null;
    }
    queryStatus.submitTime = parseDateTime(statement.columnString(4));
    queryStatus.startTime = parseDateTime(statement.columnString(5));
    queryStatus.finishTime = parseDateTime(statement.columnString(6));
    if (!statement.columnNull(7)) {
      queryStatus.elapsedNanos = statement.columnLong(7);
    }
    queryStatus.status = QueryStatusEncoding.Status.valueOf(statement.columnString(8));
    queryStatus.message = statement.columnString(9);
    List<ProfilingMode> modes = new ArrayList<ProfilingMode>();
    for (String mode : statement.columnString(10).split(",")) {
      if (!mode.equals("")) {
        modes.add(ProfilingMode.valueOf(mode));
      }
    }
    queryStatus.profilingMode = ImmutableList.copyOf(modes);
    queryStatus.ftMode = FTMode.valueOf(statement.columnString(11));
    if (!statement.columnNull(12)) {
      queryStatus.language = statement.columnString(12);
    }
    return queryStatus;
  }

  /**
   * Get the simple status (no logical or physical plan) for all queries in the system.
   * 
   * @param limit the maximum number of results to return. Any value <= 0 is interpreted as all results.
   * @param maxId return only queries with queryId <= maxId. Any value <= 0 is interpreted as no maximum.
   * @return a list of the status of all queries.
   * @throws CatalogException if there is an error in the MasterCatalog.
   */
  public List<QueryStatusEncoding> getQueries(final long limit, final long maxId) throws CatalogException {
    if (isClosed) {
      throw new CatalogException("MasterCatalog is closed.");
    }

    try {
      return queue.execute(new SQLiteJob<List<QueryStatusEncoding>>() {
        @Override
        protected List<QueryStatusEncoding> job(final SQLiteConnection sqliteConnection) throws CatalogException,
            SQLiteException {
          try {
            /* The base of the query */
            StringBuilder sb =
                new StringBuilder(
                    "SELECT query_id,raw_query,submit_time,start_time,finish_time,elapsed_nanos,status,message,profiling_mode FROM queries");
            /* The query arguments, if any. */
            List<Long> bound = Lists.newLinkedList();
            /* If there is a max query id, add the WHERE clause. */
            if (maxId > 0) {
              sb.append(" WHERE query_id <= ?");
              bound.add(maxId);
            }

            sb.append(" ORDER BY query_id DESC");

            /* If there is a limit supplied, add the LIMIT clause */
            if (limit > 0) {
              sb.append(" LIMIT ?");
              bound.add(limit);
            }
            sb.append(";");

            /* Build it and bind any arguments present. */
            SQLiteStatement statement = sqliteConnection.prepare(sb.toString());
            int argPos = 1;
            for (Long arg : bound) {
              statement.bind(argPos, arg);
              argPos++;
            }
            /* Step it. */
            statement.step();

            /* Return the results. */
            List<QueryStatusEncoding> ret = new LinkedList<QueryStatusEncoding>();
            while (statement.hasRow()) {
              ret.add(querySimpleStatusHelper(statement));
              statement.step();
            }
            statement.dispose();
            return ret;
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
   * @param relationKey the name of the relation.
   * @param storedRelationId the id of the stored relation (copy of the relation we want to read).
   * @return the list of workers that are involved in storing this relation.
   * @throws CatalogException if there is an error in the database.
   */
  public Set<Integer> getWorkersForRelation(final RelationKey relationKey, final Integer storedRelationId)
      throws CatalogException {
    Objects.requireNonNull(relationKey);
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    try {
      return queue.execute(new SQLiteJob<Set<Integer>>() {
        @Override
        protected Set<Integer> job(final SQLiteConnection sqliteConnection) throws CatalogException, SQLiteException {
          try {
            Integer relationId = storedRelationId;
            /* First, if the storedRelationId is null we pick the first copy of this relation. */
            if (storedRelationId == null) {
              SQLiteStatement statement =
                  sqliteConnection
                      .prepare("SELECT MIN(stored_relation_id) FROM stored_relations WHERE user_name = ? AND program_name = ? AND relation_name = ?;");
              statement.bind(1, relationKey.getUserName());
              statement.bind(2, relationKey.getProgramName());
              statement.bind(3, relationKey.getRelationName());
              if (!statement.step()) {
                statement.dispose();
                return null;
              }
              relationId = statement.columnInt(0);
              statement.dispose();
            }
            /* Get the list of associated workers. */
            SQLiteStatement statement =
                sqliteConnection.prepare("SELECT worker_id FROM shards WHERE stored_relation_id = ?;");
            statement.bind(1, relationId);
            Set<Integer> ret = new HashSet<Integer>();
            while (statement.step()) {
              ret.add(statement.columnInt(0));
            }
            statement.dispose();
            if (ret.size() == 0) {
              return null;
            }
            return ret;
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
   * Update the status of the specified query in the MasterCatalog.
   * 
   * @param query the state of the query.
   * @throws CatalogException if there is an error in the MasterCatalog.
   */
  public void queryFinished(final Query query) throws CatalogException {
    Objects.requireNonNull(query, "query");
    if (isClosed) {
      throw new CatalogException("MasterCatalog is closed.");
    }

    try {
      queue.execute(new SQLiteJob<Object>() {
        @Override
        protected Object job(final SQLiteConnection sqliteConnection) throws CatalogException, SQLiteException {
          try {
            SQLiteStatement statement =
                sqliteConnection
                    .prepare("UPDATE queries SET start_time=?, finish_time=?, elapsed_nanos=?, status=?, message=? WHERE query_id=?;");
            statement.bind(1, toStringOrNull(query.getStartTime()));
            statement.bind(2, toStringOrNull(query.getEndTime()));
            if (query.getElapsedTime() == null) {
              statement.bindNull(3);
            } else {
              statement.bind(3, query.getElapsedTime());
            }
            statement.bind(4, query.getStatus().toString());
            statement.bind(5, query.getMessage());
            statement.bind(6, query.getQueryId());
            statement.stepThrough();
            statement.dispose();
            return null;
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
   * Get the metadata about a relation.
   * 
   * @param relationKey specified which relation to get the metadata about.
   * @return the metadata of the specified relation.
   * @throws CatalogException if there is an error in the catalog.
   */
  public DatasetStatus getDatasetStatus(final RelationKey relationKey) throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    /* Do the work */
    try {
      return queue.execute(new SQLiteJob<DatasetStatus>() {
        @Override
        protected DatasetStatus job(final SQLiteConnection sqliteConnection) throws CatalogException, SQLiteException {
          try {
            SQLiteStatement statement =
                sqliteConnection
                    .prepare("SELECT num_tuples, query_id, finish_time FROM relations JOIN queries USING (query_id) WHERE user_name=? AND program_name=? AND relation_name=?");
            statement.bind(1, relationKey.getUserName());
            statement.bind(2, relationKey.getProgramName());
            statement.bind(3, relationKey.getRelationName());
            if (!statement.step()) {
              return null;
            }
            Schema schema = getDatasetSchema(sqliteConnection, relationKey);
            long numTuples = statement.columnLong(0);
            long queryId = statement.columnLong(1);
            String created = statement.columnString(2);
            statement.dispose();
            return new DatasetStatus(relationKey, schema, numTuples, queryId, created);
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
   * @return number of queries in catalog.
   * @throws CatalogException if an error occurs
   */
  public int getNumQueries() throws CatalogException {
    try {
      return queue.execute(new SQLiteJob<Integer>() {
        @Override
        protected Integer job(final SQLiteConnection sqliteConnection) throws CatalogException, SQLiteException {
          try {
            /* Getting this out is a simple query, which does not need to be cached. */
            final SQLiteStatement statement = sqliteConnection.prepare("SELECT count(*) FROM queries;", false);
            Preconditions.checkArgument(statement.step(), "Count should return a row");
            final Integer ret = statement.columnInt(0);
            statement.dispose();
            return ret;
          } catch (final SQLiteException e) {
            if (LOGGER.isErrorEnabled()) {
              LOGGER.error("Getting the number of queries", e);
            }
            throw new CatalogException(e);
          }
        }
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Get matching relation keys. Matching means that the search term appears somewhere in the whole relation key string.
   * Matching is fuzzy and accepts gaps in the match.
   * 
   * @param searchTerm the search term
   * @return matching relation keys or empty list
   * @throws CatalogException if an error occurs
   */
  public List<RelationKey> getMatchingRelationKeys(final String searchTerm) throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    final String expandedSearchTerm =
        '%' + Joiner.on("%").join(Splitter.fixedLength(1).split(searchTerm.toLowerCase())) + '%';

    /* Do the work */
    try {
      return queue.execute(new SQLiteJob<List<RelationKey>>() {
        @Override
        protected List<RelationKey> job(final SQLiteConnection sqliteConnection) throws CatalogException,
            SQLiteException {
          try {
            SQLiteStatement statement =
                sqliteConnection.prepare("SELECT user_name, program_name, relation_name FROM relations "
                    + "WHERE lower(user_name || ':' || program_name || ':' || relation_name) LIKE ?");
            statement.bind(1, expandedSearchTerm);

            ImmutableList.Builder<RelationKey> result = ImmutableList.builder();

            while (statement.step()) {
              String userName = statement.columnString(0);
              String programName = statement.columnString(1);
              String relationName = statement.columnString(2);
              result.add(new RelationKey(userName, programName, relationName));
            }
            statement.dispose();
            return result.build();
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
   * Delete the specified relation from the catalog, if it exists.
   * 
   * @param relation the relation to be deleted.
   * @throws CatalogException if there is an error
   */
  public void deleteRelationIfExists(final RelationKey relation) throws CatalogException {
    Objects.requireNonNull(relation, "relation");
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    /* Do the work */
    try {
      queue.execute(new SQLiteJob<Void>() {
        @Override
        protected Void job(final SQLiteConnection sqliteConnection) throws CatalogException, SQLiteException {
          try {
            SQLiteStatement statement =
                sqliteConnection
                    .prepare("DELETE FROM relations WHERE user_name=? AND program_name=? AND relation_name=?;");
            statement.bind(1, relation.getUserName());
            statement.bind(2, relation.getProgramName());
            statement.bind(3, relation.getRelationName());
            statement.stepThrough();
            statement.dispose();
            statement = null;
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
   * Update the {@link MasterCatalog} so that the specified relation has the specified tuple count.
   * 
   * @param relation the relation to update
   * @param count the number of tuples in that relation
   * @throws CatalogException if there is an error
   */
  public void updateRelationTupleCount(final RelationKey relation, final long count) throws CatalogException {
    Objects.requireNonNull(relation, "relation");
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    /* Do the work */
    try {
      queue.execute(new SQLiteJob<Void>() {
        @Override
        protected Void job(final SQLiteConnection sqliteConnection) throws CatalogException, SQLiteException {
          try {
            SQLiteStatement statement =
                sqliteConnection
                    .prepare("UPDATE relations SET num_tuples=? WHERE user_name=? AND program_name=? AND relation_name=?;");
            statement.bind(1, count);
            statement.bind(2, relation.getUserName());
            statement.bind(3, relation.getProgramName());
            statement.bind(4, relation.getRelationName());
            statement.stepThrough();
            statement.dispose();
            statement = null;
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
}
