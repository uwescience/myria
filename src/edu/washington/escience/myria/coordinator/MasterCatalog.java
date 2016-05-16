package edu.washington.escience.myria.coordinator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.io.FilenameUtils;
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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import edu.washington.escience.myria.MyriaConstants.FTMode;
import edu.washington.escience.myria.MyriaConstants.ProfilingMode;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.accessmethod.SQLiteTupleBatchIterator;
import edu.washington.escience.myria.api.MyriaJsonMapperProvider;
import edu.washington.escience.myria.api.encoding.DatasetStatus;
import edu.washington.escience.myria.api.encoding.QueryEncoding;
import edu.washington.escience.myria.api.encoding.QueryStatusEncoding;
import edu.washington.escience.myria.api.encoding.plan.SubPlanEncoding;
import edu.washington.escience.myria.operator.network.partition.HowPartitioned;
import edu.washington.escience.myria.parallel.Query;
import edu.washington.escience.myria.parallel.RelationWriteMetadata;
import edu.washington.escience.myria.parallel.SubQueryId;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * This class is intended to store the configuration information for a Myria installation.
 *
 *
 */
public final class MasterCatalog {
  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(MasterCatalog.class);

  /** CREATE TABLE statements @formatter:off */
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
  /** Create the query full-text search table. */
  private static final String CREATE_QUERIES_FTS =
      "CREATE VIRTUAL TABLE queries_fts USING FTS4(\n"
          + "    query_id INTEGER NOT NULL PRIMARY KEY ASC REFERENCES queries (query_id),\n"
          + "    raw_query_fts TEXT NOT NULL);";
  /** Create the query plans table. */
  private static final String CREATE_QUERY_PLANS =
      "CREATE TABLE query_plans (\n"
          + "    query_id INTEGER NOT NULL,\n"
          + "    subquery_id INTEGER NOT NULL,\n"
          + "    plan TEXT NOT NULL,\n"
          + "    PRIMARY KEY (query_id, subquery_id)\n"
          + ");";
  /** Create the relations table. */
  private static final String CREATE_RELATIONS =
      "CREATE TABLE relations (\n"
          + "    user_name TEXT NOT NULL,\n"
          + "    program_name TEXT NOT NULL,\n"
          + "    relation_name TEXT NOT NULL,\n"
          + "    num_tuples INTEGER NOT NULL,\n"
          + "    is_deleted INTEGER NOT NULL,\n"
          + "    is_persistent INTEGER NOT NULL,\n"
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
  /** Create an index on the relation_schema table. */
  private static final String CREATE_RELATION_SCHEMA_INDEX =
      "CREATE INDEX relation_schema_idx ON relation_schema (\n"
          + "    user_name, program_name, relation_name, col_index);";
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
  /** Create an index on the stored_relations table. */
  private static final String CREATE_STORED_RELATIONS_INDEX =
      "CREATE INDEX stored_relations_idx ON stored_relations (\n"
          + "    user_name, program_name, relation_name);";
  /** Create the stored_relations table. */
  private static final String CREATE_SHARDS =
      "CREATE TABLE shards (\n"
          + "    stored_relation_id INTEGER NOT NULL REFERENCES stored_relations ON DELETE CASCADE,\n"
          + "    shard_index INTEGER NOT NULL,\n"
          + "    worker_id INTEGER NOT NULL);";
  /** Create an index on the shards table. */
  private static final String CREATE_SHARDS_INDEX =
      "CREATE INDEX shards_idx ON shards (stored_relation_id);";
  /** Create the stored_relations table. */
  private static final String UPDATE_UNKNOWN_STATUS =
      "UPDATE queries "
          + "SET status = '"
          + QueryStatusEncoding.Status.UNKNOWN.toString()
          + "'"
          + "WHERE status = '"
          + QueryStatusEncoding.Status.ACCEPTED.toString()
          + "';";
  /** CREATE TABLE statements @formatter:on */

  /**
   * @param filename the path to the SQLite database storing the catalog.
   * @return a fresh Catalog fitting the specified description.
   * @throws CatalogException if there is an error opening the database.
   *
   *           TODO add some sanity checks to the filename?
   */
  public static void create(@Nonnull final String path) throws CatalogException {
    try {
      Files.createDirectories(FileSystems.getDefault().getPath(path));
    } catch (IOException e) {
      throw new CatalogException(e);
    }
    MasterCatalog.createFromFile(new File(FilenameUtils.concat(path, "master.catalog"))).close();
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
      queue
          .execute(
              new SQLiteJob<Object>() {

                @Override
                protected Object job(final SQLiteConnection sqliteConnection)
                    throws SQLiteException, CatalogException {
                  /* Create all the tables in the Catalog. */
                  try {
                    sqliteConnection.exec("PRAGMA journal_mode = WAL;");
                    sqliteConnection.exec("BEGIN TRANSACTION");
                    sqliteConnection.exec(CREATE_QUERIES);
                    sqliteConnection.exec(CREATE_QUERIES_FTS);
                    sqliteConnection.exec(CREATE_QUERY_PLANS);
                    sqliteConnection.exec(CREATE_RELATIONS);
                    sqliteConnection.exec(CREATE_RELATION_SCHEMA);
                    sqliteConnection.exec(CREATE_RELATION_SCHEMA_INDEX);
                    sqliteConnection.exec(CREATE_STORED_RELATIONS);
                    sqliteConnection.exec(CREATE_STORED_RELATIONS_INDEX);
                    sqliteConnection.exec(CREATE_SHARDS);
                    sqliteConnection.exec(CREATE_SHARDS_INDEX);
                    sqliteConnection.exec("END TRANSACTION");
                  } catch (final SQLiteException e) {
                    sqliteConnection.exec("ROLLBACK TRANSACTION");
                    LOGGER.error("Creating catalog tables", e);
                    throw new CatalogException(
                        "SQLiteException while creating new Catalog tables", e);
                  }
                  return null;
                }
              })
          .get();
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
  public static MasterCatalog open(final String filename)
      throws FileNotFoundException, CatalogException {
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
      queue
          .execute(
              new SQLiteJob<Object>() {
                @Override
                protected Object job(final SQLiteConnection sqliteConnection)
                    throws SQLiteException {
                  sqliteConnection.exec("PRAGMA foreign_keys = ON;");
                  sqliteConnection.exec("BEGIN EXCLUSIVE;");
                  sqliteConnection.exec("COMMIT;");
                  sqliteConnection.exec(UPDATE_UNKNOWN_STATUS);
                  return null;
                }
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
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
      return queue
          .execute(
              new SQLiteJob<List<RelationKey>>() {
                @Override
                protected List<RelationKey> job(final SQLiteConnection sqliteConnection)
                    throws SQLiteException, CatalogException {
                  final List<RelationKey> relations = new ArrayList<RelationKey>();

                  try {
                    final SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "SELECT user_name,program_name,relation_name FROM relations;", false);
                    while (statement.step()) {
                      relations.add(
                          RelationKey.of(
                              statement.columnString(0),
                              statement.columnString(1),
                              statement.columnString(2)));
                    }
                    statement.dispose();
                  } catch (final SQLiteException e) {
                    LOGGER.error(e.toString());
                    throw new CatalogException(e);
                  }

                  return relations;
                }
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Private helper to add the metadata for a relation into the Catalog.
   *
   * @param sqliteConnection the connection to the SQLite database.
   * @param relation the relation to create.
   * @param schema the schema of the relation.
   * @param numTuples the number of tuples in the relation.
   * @param queryId the query that created the relation.
   * @throws CatalogException if the relation is already in the catalog or there is an error in the database.
   */
  private void addRelationMetadata(
      @Nonnull final SQLiteConnection sqliteConnection,
      @Nonnull final RelationKey relation,
      @Nonnull final Schema schema,
      final long numTuples,
      final long queryId)
      throws CatalogException {
    try {
      /* First, insert the relation name. */
      SQLiteStatement statement =
          sqliteConnection.prepare(
              "INSERT INTO relations (user_name,program_name,relation_name,num_tuples,is_deleted,is_persistent,query_id) VALUES (?,?,?,?,?,?,?);");
      statement.bind(1, relation.getUserName());
      statement.bind(2, relation.getProgramName());
      statement.bind(3, relation.getRelationName());
      statement.bind(4, numTuples);
      statement.bind(5, 0);
      statement.bind(6, 0);
      statement.bind(7, queryId);
      statement.stepThrough();
      statement.dispose();
      statement = null;

      /* Second, populate the Schema table. */
      statement =
          sqliteConnection.prepare(
              "INSERT INTO relation_schema(user_name,program_name,relation_name,col_index,col_name,col_type) "
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
    } catch (final SQLiteException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Private helper to add the metadata for a relation into the Catalog.
   *
   * @param sqliteConnection the connection to the SQLite database.
   * @param relation the relation to create.
   * @param workers the IDs of the workers storing this copy of the relation.
   * @param howPartitioned how this copy of the relation is partitioned.
   * @throws CatalogException if there is an error in the database.
   */
  private void addStoredRelation(
      final SQLiteConnection sqliteConnection,
      final RelationKey relation,
      final Set<Integer> workers,
      final HowPartitioned howPartitioned)
      throws CatalogException {
    try {
      /* First, populate the stored_relation table. */
      SQLiteStatement statement =
          sqliteConnection.prepare(
              "INSERT INTO stored_relations (user_name,program_name,relation_name,num_shards,how_partitioned) VALUES (?,?,?,?,?);");
      statement.bind(1, relation.getUserName());
      statement.bind(2, relation.getProgramName());
      statement.bind(3, relation.getRelationName());
      statement.bind(4, workers.size());
      try {
        statement.bind(5, MyriaJsonMapperProvider.getMapper().writeValueAsString(howPartitioned));
      } catch (JsonProcessingException e) {
        throw new CatalogException(e);
      }
      statement.stepThrough();
      statement.dispose();
      statement = null;

      Long storedRelationId = sqliteConnection.getLastInsertId();
      /* Second, populate the shards table. */
      statement =
          sqliteConnection.prepare(
              "INSERT INTO shards(stored_relation_id,shard_index,worker_id) " + "VALUES (?,?,?);");
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
    } catch (final SQLiteException e) {
      throw new CatalogException(e);
    }
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
   * Helper function to get schema for a relation from the catalog.
   *
   * @param sqliteConnection the connection to the database
   * @param relationKey the relation for which schema is desired
   * @return the schema of the relation
   * @throws CatalogException if there is an error in the catalog.
   */
  private Schema getSchema(
      @Nonnull final SQLiteConnection sqliteConnection, @Nonnull final RelationKey relationKey)
      throws CatalogException {
    try {
      SQLiteStatement statement =
          sqliteConnection.prepare(
              "SELECT col_name,col_type FROM relation_schema WHERE user_name=? AND program_name=? AND relation_name=? ORDER BY col_index ASC");
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

  /**
   * @param relationKey the key of the desired relation.
   * @return the schema of the specified relation, or null if not found.
   * @throws CatalogException if there is an error accessing the desired Schema.
   */
  public Schema getSchema(@Nonnull final RelationKey relationKey) throws CatalogException {
    Objects.requireNonNull(relationKey);
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    /* Do the work */
    try {
      return queue
          .execute(
              new SQLiteJob<Schema>() {
                @Override
                protected Schema job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  return getSchema(sqliteConnection, relationKey);
                }
              })
          .get();
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
      return queue
          .execute(
              new SQLiteJob<List<DatasetStatus>>() {
                @Override
                protected List<DatasetStatus> job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "SELECT user_name, program_name, relation_name, num_tuples, query_id, finish_time, how_partitioned FROM relations JOIN queries USING (query_id) JOIN stored_relations USING (user_name,program_name,relation_name) WHERE is_deleted=0 ORDER BY user_name, program_name, relation_name ASC");
                    return datasetStatusListHelper(statement, sqliteConnection);
                  } catch (final SQLiteException e) {
                    throw new CatalogException(e);
                  }
                }
              })
          .get();
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
      return queue
          .execute(
              new SQLiteJob<List<DatasetStatus>>() {
                @Override
                protected List<DatasetStatus> job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "SELECT user_name, program_name, relation_name, num_tuples, query_id, finish_time, how_partitioned FROM relations JOIN queries USING (query_id) JOIN stored_relations USING (user_name,program_name,relation_name) WHERE user_name=? AND is_deleted=0 ORDER BY user_name, program_name, relation_name ASC");
                    statement.bind(1, userName);
                    return datasetStatusListHelper(statement, sqliteConnection);
                  } catch (final SQLiteException e) {
                    throw new CatalogException(e);
                  }
                }
              })
          .get();
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
      return queue
          .execute(
              new SQLiteJob<List<DatasetStatus>>() {
                @Override
                protected List<DatasetStatus> job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "SELECT user_name, program_name, relation_name, num_tuples, query_id, finish_time, how_partitioned FROM relations JOIN queries USING (query_id) JOIN stored_relations USING (user_name,program_name,relation_name) WHERE user_name=? AND program_name=? AND is_deleted=0 ORDER BY user_name, program_name, relation_name ASC");
                    statement.bind(1, userName);
                    statement.bind(2, programName);
                    return datasetStatusListHelper(statement, sqliteConnection);
                  } catch (final SQLiteException e) {
                    throw new CatalogException(e);
                  }
                }
              })
          .get();
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
      return queue
          .execute(
              new SQLiteJob<List<DatasetStatus>>() {
                @Override
                protected List<DatasetStatus> job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "SELECT user_name, program_name, relation_name, num_tuples, query_id, finish_time, how_partitioned FROM relations JOIN queries USING (query_id) JOIN stored_relations USING (user_name,program_name,relation_name) WHERE query_id=? AND is_deleted=0 ORDER BY user_name, program_name, relation_name ASC");
                    statement.bind(1, queryId);
                    return datasetStatusListHelper(statement, sqliteConnection);
                  } catch (final SQLiteException e) {
                    throw new CatalogException(e);
                  }
                }
              })
          .get();
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
  private static List<DatasetStatus> datasetStatusListHelper(
      final SQLiteStatement statement, final SQLiteConnection connection) throws CatalogException {
    try {
      ImmutableList.Builder<DatasetStatus> result = ImmutableList.builder();
      while (statement.step()) {
        RelationKey relationKey =
            RelationKey.of(
                statement.columnString(0), statement.columnString(1), statement.columnString(2));
        long numTuples = statement.columnLong(3);
        long queryId = statement.columnLong(4);
        String created = statement.columnString(5);
        HowPartitioned howPartitioned;
        try {
          howPartitioned =
              MyriaJsonMapperProvider.getMapper()
                  .readValue(statement.columnString(6), HowPartitioned.class);
        } catch (final IOException e) {
          LOGGER.debug("Error deserializing howPartitioned for dataset #{}", relationKey, e);
          howPartitioned = new HowPartitioned(null, null);
        }
        result.add(
            new DatasetStatus(
                relationKey,
                getDatasetSchema(connection, relationKey),
                numTuples,
                queryId,
                created,
                howPartitioned));
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
  private static Schema getDatasetSchema(
      final SQLiteConnection sqliteConnection, final RelationKey relationKey)
      throws CatalogException {
    try {
      SQLiteStatement statement =
          sqliteConnection.prepare(
              "SELECT col_name, col_type FROM relation_schema JOIN relations USING (user_name,program_name,relation_name) WHERE user_name=? AND program_name=? AND relation_name=? and is_deleted=0 ORDER BY col_index ASC");
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
      return queue
          .execute(
              new SQLiteJob<Long>() {
                @Override
                protected Long job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "INSERT INTO queries (raw_query, logical_ra, plan, submit_time, start_time, finish_time, elapsed_nanos, status, profiling_mode, ft_mode, language) VALUES (?,?,?,?,?,?,?,?,?,?,?);");
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
                    String modes = Joiner.on(',').join(queryStatus.profilingMode);
                    statement.bind(9, modes);
                    statement.bind(10, queryStatus.ftMode.toString());
                    statement.bind(11, queryStatus.language);
                    statement.stepThrough();
                    statement.dispose();
                    Long queryId = sqliteConnection.getLastInsertId();

                    // Full-text search also
                    statement =
                        sqliteConnection.prepare(
                            "INSERT INTO queries_fts (query_id, raw_query_fts) VALUES (?,?);");
                    statement.bind(1, queryId);
                    statement.bind(2, queryStatus.rawQuery);
                    statement.stepThrough();
                    statement.dispose();
                    return queryId;
                  } catch (final SQLiteException e) {
                    throw new CatalogException(e);
                  }
                }
              })
          .get();
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
      return queue
          .execute(
              new SQLiteJob<QueryStatusEncoding>() {
                @Override
                protected QueryStatusEncoding job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "SELECT query_id,raw_query,logical_ra,plan,submit_time,start_time,finish_time,elapsed_nanos,status,message,profiling_mode,ft_mode,language FROM queries WHERE query_id=?;");
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
              })
          .get();
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
  private static QueryStatusEncoding querySimpleStatusHelper(final SQLiteStatement statement)
      throws SQLiteException {
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
    queryStatus.language = statement.columnString(9);
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
  private static QueryStatusEncoding queryStatusHelper(final SQLiteStatement statement)
      throws SQLiteException, IOException {
    final QueryStatusEncoding queryStatus = new QueryStatusEncoding(statement.columnLong(0));
    queryStatus.rawQuery = statement.columnString(1);
    queryStatus.logicalRa = statement.columnString(2);
    String physicalString = statement.columnString(3);

    try {
      queryStatus.plan =
          MyriaJsonMapperProvider.getMapper().readValue(physicalString, SubPlanEncoding.class);
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
   * Helper function to bind arguments to their corresponding positions in a SQLite statement. Object types are used to
   * determine which version of the {@link SQLiteStatement#bind} function is to be used.
   *
   * @param statement the SQLite statement
   * @param args a list of the arguments to be bound
   * @param startPos the starting position at which to bind arguments in the statement
   * @throws SQLiteException if something goes wrong.
   */
  private static void bindArgs(
      @Nonnull final SQLiteStatement statement,
      @Nonnull final List<Object> args,
      final int startPos)
      throws SQLiteException {
    Preconditions.checkNotNull(statement, "statement");
    Preconditions.checkNotNull(args, "args");
    Preconditions.checkArgument(
        startPos >= 1, "starting index position must be >= 1 [%s]", startPos);

    int pos = startPos;
    for (Object arg : args) {
      if (arg == null) {
        statement.bindNull(pos);
      } else if (arg instanceof Double) {
        statement.bind(pos, (Double) arg);
      } else if (arg instanceof Float) {
        statement.bind(pos, (Float) arg);
      } else if (arg instanceof Integer) {
        statement.bind(pos, (Integer) arg);
      } else if (arg instanceof Long) {
        statement.bind(pos, (Long) arg);
      } else if (arg instanceof String) {
        statement.bind(pos, (String) arg);
      } else {
        throw new IllegalArgumentException(
            "Unexpected SQLite parameter of type " + arg.getClass() + " at position pos");
      }
      pos++;
    }
  }

  /**
   * Get the simple status (no logical or physical plan) for all queries in the system.
   *
   * @param limit the maximum number of results to return. Any value <= 0 is interpreted as all results.
   * @param maxId return only queries with queryId <= maxId. Any value <= 0 is interpreted as no maximum.
   * @param minId return only queries with queryId >= minId. Any value <= 0 is interpreted as no minimum. Ignored if
   *          maxId is present.
   * @param searchTerm a token to match against the raw queries. If null, all queries will be returned.
   * @return a list of the status of all queries.
   * @throws CatalogException if there is an error in the MasterCatalog.
   */
  public List<QueryStatusEncoding> getQueries(
      @Nullable final Long limit,
      @Nullable final Long maxId,
      @Nullable final Long minId,
      @Nullable final String searchTerm)
      throws CatalogException {
    if (isClosed) {
      throw new CatalogException("MasterCatalog is closed.");
    }

    Preconditions.checkArgument(
        searchTerm == null || searchTerm.length() >= 3,
        "when present, search term must be at least 3 characters long [given: %s]",
        searchTerm);

    /* The query arguments, if any. */
    final List<Object> bindArgs = Lists.newLinkedList();
    List<String> whereClause = Lists.newLinkedList();

    /* Is there a search? */
    String fromClause = "FROM queries";
    if (searchTerm != null) {
      fromClause = "FROM queries JOIN queries_fts USING (query_id)";
      whereClause.add("raw_query_fts MATCH ?");
      bindArgs.add(searchTerm);
    }

    /* Is there a max? */
    boolean min = false;
    if (maxId != null && maxId > 0) {
      whereClause.add("query_id <= ?");
      bindArgs.add(maxId);
    } else if (minId != null && minId > 0) {
      whereClause.add("query_id >= ?");
      bindArgs.add(minId);
      min = true;
    }

    String selectClause =
        "SELECT query_id,raw_query,submit_time,start_time,finish_time,elapsed_nanos,status,message,profiling_mode,language";
    StringBuilder coreQuery = new StringBuilder();
    Joiner.on(' ').appendTo(coreQuery, selectClause, fromClause);
    if (whereClause.size() > 0) {
      coreQuery.append(" WHERE ");
      Joiner.on(" AND ").appendTo(coreQuery, whereClause);
    }

    if (limit != null && limit > 0) {
      if (!min) {
        coreQuery.append(" ORDER BY query_id DESC LIMIT ?");
        bindArgs.add(limit);
      } else {
        coreQuery.append(" ORDER BY query_id ASC LIMIT ?");
        bindArgs.add(limit);
        coreQuery =
            new StringBuilder("SELECT * FROM (")
                .append(coreQuery)
                .append(") ORDER BY query_id DESC");
      }
    } else {
      coreQuery.append(" ORDER BY query_id DESC");
    }

    final String query = coreQuery.toString();

    try {
      return queue
          .execute(
              new SQLiteJob<List<QueryStatusEncoding>>() {
                @Override
                protected List<QueryStatusEncoding> job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    /* Build it and bind any arguments present. */
                    SQLiteStatement statement = sqliteConnection.prepare(query);
                    bindArgs(statement, bindArgs, 1);

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
              })
          .get();
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
  public Set<Integer> getWorkersForRelation(
      final RelationKey relationKey, final Integer storedRelationId) throws CatalogException {
    Objects.requireNonNull(relationKey);
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    try {
      return queue
          .execute(
              new SQLiteJob<Set<Integer>>() {
                @Override
                protected Set<Integer> job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    Integer relationId = storedRelationId;
                    /* First, if the storedRelationId is null we pick the first copy of this relation. */
                    if (storedRelationId == null) {
                      SQLiteStatement statement =
                          sqliteConnection.prepare(
                              "SELECT MIN(stored_relation_id) FROM stored_relations WHERE user_name = ? AND program_name = ? AND relation_name = ?;");
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
                        sqliteConnection.prepare(
                            "SELECT worker_id FROM shards WHERE stored_relation_id = ?;");
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
              })
          .get();
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
      queue
          .execute(
              new SQLiteJob<Object>() {
                @Override
                protected Object job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "UPDATE queries SET start_time=?, finish_time=?, elapsed_nanos=?, status=?, message=? WHERE query_id=?;");
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
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Update the partition function of an ingested dataset.
   *
   * @param key the relation key.
   * @param howPartitioned how the dataset was partitioned.
   * @throws CatalogException if there is an error in the catalog.
   */
  public void updateHowPartitioned(final RelationKey key, final HowPartitioned howPartitioned)
      throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    try {
      queue
          .execute(
              new SQLiteJob<DatasetStatus>() {
                @Override
                protected DatasetStatus job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "UPDATE stored_relations set how_partitioned=? WHERE user_name=? AND program_name=? AND relation_name=?");
                    try {
                      statement.bind(
                          1,
                          MyriaJsonMapperProvider.getMapper().writeValueAsString(howPartitioned));
                    } catch (JsonProcessingException e) {
                      throw new CatalogException(e);
                    }
                    statement.bind(2, key.getUserName());
                    statement.bind(3, key.getProgramName());
                    statement.bind(4, key.getRelationName());
                    statement.stepThrough();
                    statement.dispose();
                    statement = null;
                  } catch (final SQLiteException e) {
                    throw new CatalogException(e);
                  }
                  return null;
                }
              })
          .get();
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
      return queue
          .execute(
              new SQLiteJob<DatasetStatus>() {
                @Override
                protected DatasetStatus job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "SELECT num_tuples, query_id, finish_time, how_partitioned FROM relations JOIN queries USING (query_id) JOIN stored_relations USING (user_name,program_name,relation_name) WHERE user_name=? AND program_name=? AND relation_name=?");
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
                    HowPartitioned howPartitioned;
                    try {
                      howPartitioned =
                          MyriaJsonMapperProvider.getMapper()
                              .readValue(statement.columnString(3), HowPartitioned.class);
                    } catch (final IOException e) {
                      LOGGER.debug(
                          "Error deserializing howPartitioned for dataset #{}",
                          relationKey.toString(),
                          e);
                      howPartitioned = new HowPartitioned(null, null);
                    }
                    statement.dispose();
                    return new DatasetStatus(
                        relationKey, schema, numTuples, queryId, created, howPartitioned);
                  } catch (final SQLiteException e) {
                    throw new CatalogException(e);
                  }
                }
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * @return the maximum query id that matches the search.
   * @param searchTerm a token to match against the raw queries. If null, all queries match.
   * @throws CatalogException if an error occurs
   */
  public long getMaxQuery(@Nullable final String searchTerm) throws CatalogException {
    Preconditions.checkArgument(
        searchTerm == null || searchTerm.length() >= 3,
        "when present, search term must be at least 3 characters long [given: %s]",
        searchTerm);

    try {
      return queue
          .execute(
              new SQLiteJob<Long>() {
                @Override
                protected Long job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    /* Getting this out is a simple query, which does not need to be cached. */
                    final SQLiteStatement statement;
                    if (searchTerm == null) {
                      statement =
                          sqliteConnection.prepare("SELECT max(query_id) FROM queries_fts;");
                    } else {
                      statement =
                          sqliteConnection.prepare(
                              "SELECT max(query_id) FROM queries_fts WHERE raw_query_fts MATCH ?;");
                      statement.bind(1, searchTerm);
                    }

                    Preconditions.checkArgument(statement.step(), "Count should return a row");
                    final Long ret = statement.columnLong(0);
                    statement.dispose();
                    return ret;
                  } catch (final SQLiteException e) {
                    LOGGER.error("Getting the max query", e);
                    throw new CatalogException(e);
                  }
                }
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * @return the minimum query id that matches the search.
   * @param searchTerm a token to match against the raw queries. If null, all queries match.
   * @throws CatalogException if an error occurs
   */
  public long getMinQuery(@Nullable final String searchTerm) throws CatalogException {
    Preconditions.checkArgument(
        searchTerm == null || searchTerm.length() >= 3,
        "when present, search term must be at least 3 characters long [given: %s]",
        searchTerm);

    try {
      return queue
          .execute(
              new SQLiteJob<Long>() {
                @Override
                protected Long job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    /* Getting this out is a simple query, which does not need to be cached. */
                    final SQLiteStatement statement;
                    if (searchTerm == null) {
                      statement =
                          sqliteConnection.prepare("SELECT min(query_id) FROM queries_fts;");
                    } else {
                      statement =
                          sqliteConnection.prepare(
                              "SELECT min(query_id) FROM queries_fts WHERE raw_query_fts MATCH ?;");
                      statement.bind(1, searchTerm);
                    }

                    Preconditions.checkArgument(statement.step(), "Count should return a row");
                    final Long ret = statement.columnLong(0);
                    statement.dispose();
                    return ret;
                  } catch (final SQLiteException e) {
                    LOGGER.error("Getting the min query", e);
                    throw new CatalogException(e);
                  }
                }
              })
          .get();
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
  public List<RelationKey> getMatchingRelationKeys(final String searchTerm)
      throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    final String expandedSearchTerm =
        '%' + Joiner.on("%").join(Splitter.fixedLength(1).split(searchTerm.toLowerCase())) + '%';

    /* Do the work */
    try {
      return queue
          .execute(
              new SQLiteJob<List<RelationKey>>() {
                @Override
                protected List<RelationKey> job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "SELECT user_name, program_name, relation_name FROM relations "
                                + "WHERE lower(user_name || ':' || program_name || ':' || relation_name) LIKE ? AND is_deleted=0");
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
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Delete the specified relation from the catalog, if it exists.
   *
   * @param sqliteConnection the connection to the SQLite database
   * @param relation the relation to be deleted.
   * @throws CatalogException if there is an error
   */
  private void deleteRelationIfExists(
      @Nonnull final SQLiteConnection sqliteConnection,
      @Nonnull final RelationKey relation,
      final boolean isOverwrite)
      throws CatalogException {
    try {
      String sql =
          String.format(
              "DELETE FROM relations WHERE user_name=? AND program_name=? AND relation_name=? AND %s;",
              (isOverwrite ? "1=1" : "is_deleted=1"));
      SQLiteStatement statement = sqliteConnection.prepare(sql);
      statement.bind(1, relation.getUserName());
      statement.bind(2, relation.getProgramName());
      statement.bind(3, relation.getRelationName());
      statement.stepThrough();
      statement.dispose();
      statement = null;
    } catch (final SQLiteException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Connects to SQLite to delete relation from the catalog if it exists.
   *
   * @param relationKey the relation to be deleted from the catalog.
   * @throws CatalogException if there is an error in the catalog.
   */
  public void deleteRelationFromCatalog(final RelationKey relationKey) throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    /* Do the work */
    try {
      queue
          .execute(
              new SQLiteJob<Void>() {
                @Override
                protected Void job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  sqliteConnection.exec("BEGIN TRANSACTION;");
                  try {
                    deleteRelationIfExists(sqliteConnection, relationKey, false);
                    sqliteConnection.exec("COMMIT TRANSACTION;");
                  } catch (SQLiteException e) {
                    try {
                      sqliteConnection.exec("ROLLBACK TRANSACTION;");
                    } catch (SQLiteException e2) {
                      LOGGER.error("exception was thrown during transaction rollback ", e2);
                      assert true; /* Do nothing */
                    }
                    throw e;
                  }
                  return null;
                }
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Mark a relation as is_deleted
   *
   * @param relation the relation to be labeled
   * @throws CatalogException if there is an error
   */
  public void markRelationDeleted(@Nonnull final RelationKey relation) throws CatalogException {
    Objects.requireNonNull(relation, "relation");
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    if (isDeletedRelation(relation)) {
      LOGGER.warn("Relation has already been deleted");
    }

    /* Do the work */
    try {
      queue
          .execute(
              new SQLiteJob<Void>() {
                @Override
                protected Void job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "UPDATE relations SET is_deleted=1 WHERE user_name=? AND program_name=? AND relation_name=?;");
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
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Mark a relation as is_persistent
   *
   * @param relation the relation to be labeled
   * @throws CatalogException if there is an error
   */
  public void markRelationPersistent(@Nonnull final RelationKey relation) throws CatalogException {
    Objects.requireNonNull(relation, "relation");
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    /* Do the work */
    try {
      queue
          .execute(
              new SQLiteJob<Void>() {
                @Override
                protected Void job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "UPDATE relations SET is_persistent=1 WHERE user_name=? AND program_name=? AND relation_name=?;");
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
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Add the metadata for one or more relations to the catalog.
   *
   * @param relationsCreated which relations were created.
   * @param subQueryId which subquery is doing the creation.
   * @throws CatalogException if there is an error in the catalog.
   */
  public void updateRelationMetadata(
      @Nonnull final Map<RelationKey, RelationWriteMetadata> relationsCreated,
      @Nonnull final SubQueryId subQueryId)
      throws CatalogException {
    Objects.requireNonNull(relationsCreated, "relationsCreated");
    Objects.requireNonNull(subQueryId, "subQueryId");
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    /* Do the work */
    try {
      queue
          .execute(
              new SQLiteJob<Void>() {
                @Override
                protected Void job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  sqliteConnection.exec("BEGIN TRANSACTION;");
                  try {
                    for (RelationWriteMetadata meta : relationsCreated.values()) {
                      RelationKey relation = meta.getRelationKey();
                      Set<Integer> workers = meta.getWorkers();
                      if (meta.isOverwrite()) {
                        deleteRelationIfExists(sqliteConnection, meta.getRelationKey(), true);
                      }
                      Schema schema = meta.getSchema();
                      if (meta.isOverwrite() || getSchema(sqliteConnection, relation) == null) {
                        /* Overwrite or new relation. */
                        addRelationMetadata(
                            sqliteConnection, relation, schema, -1, subQueryId.getQueryId());
                        addStoredRelation(
                            sqliteConnection,
                            relation,
                            workers,
                            new HowPartitioned(meta.getPartitionFunction(), Ints.toArray(workers)));
                        LOGGER.debug(
                            "SubQuery #{} - adding {} to store shard of {}",
                            subQueryId,
                            workers,
                            relation);
                      }
                    }
                    sqliteConnection.exec("COMMIT TRANSACTION;");
                  } catch (SQLiteException e) {
                    try {
                      sqliteConnection.exec("ROLLBACK TRANSACTION;");
                    } catch (SQLiteException e2) {
                      assert true; /* Do nothing */
                    }
                    throw e;
                  }
                  return null;
                }
              })
          .get();
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
  public void updateRelationTupleCount(final RelationKey relation, final long count)
      throws CatalogException {
    Objects.requireNonNull(relation, "relation");
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }

    if (isDeletedRelation(relation)) {
      LOGGER.warn("Relation has already been deleted");
    }

    /* Do the work */
    try {
      queue
          .execute(
              new SQLiteJob<Void>() {
                @Override
                protected Void job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "UPDATE relations SET num_tuples=? WHERE user_name=? AND program_name=? AND relation_name=? and is_deleted=0;");
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
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Record the fact that this subquery executed this in the catalog.
   *
   * @param subQueryId the id of the subquery.
   * @param encodedPlan the plan.
   * @throws CatalogException if there is an error.
   */
  public void setQueryPlan(@Nonnull final SubQueryId subQueryId, @Nonnull final String encodedPlan)
      throws CatalogException {
    Preconditions.checkNotNull(subQueryId, "subQueryId");
    Preconditions.checkNotNull(encodedPlan, "encodedPlan");
    /* Do the work */
    try {
      queue
          .execute(
              new SQLiteJob<Void>() {
                @Override
                protected Void job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "INSERT INTO query_plans (query_id, subquery_id, plan) VALUES (?,?,?);");
                    statement.bind(1, subQueryId.getQueryId());
                    statement.bind(2, subQueryId.getSubqueryId());
                    statement.bind(3, encodedPlan);
                    statement.stepThrough();
                    statement.dispose();
                    statement = null;
                  } catch (final SQLiteException e) {
                    throw new CatalogException(e);
                  }
                  return null;
                }
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Get the saved query plan for the target subquery.
   *
   * @param subQueryId the id of the subquery.
   * @return the saved query plan for the target subquery.
   * @throws CatalogException if there is an error.
   */
  @Nullable
  public String getQueryPlan(final SubQueryId subQueryId) throws CatalogException {
    Preconditions.checkNotNull(subQueryId, "subQueryId");
    /* Do the work */
    try {
      return queue
          .execute(
              new SQLiteJob<String>() {
                @Override
                protected String job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  String ret = null;
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "SELECT plan FROM query_plans WHERE query_id=? AND subquery_id=?;");
                    statement.bind(1, subQueryId.getQueryId());
                    statement.bind(2, subQueryId.getSubqueryId());

                    if (statement.step()) {
                      ret = statement.columnString(0);
                    }
                    statement.dispose();
                    statement = null;
                  } catch (final SQLiteException e) {
                    throw new CatalogException(e);
                  }
                  return ret;
                }
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Checking whether the relation has an is_deleted status
   *
   * @param relationKey the relation to check is_deleted status.
   * @return a boolean whether the relation is in a is_deleted status.
   * @throws CatalogException if there is an error.
   */
  public Boolean isDeletedRelation(final RelationKey relationKey) throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }
    try {
      return queue
          .execute(
              new SQLiteJob<Boolean>() {
                @Override
                protected Boolean job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  int ret = 0;
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "SELECT is_deleted FROM relations WHERE user_name=? AND program_name=? AND relation_name=?;");
                    statement.bind(1, relationKey.getUserName());
                    statement.bind(2, relationKey.getProgramName());
                    statement.bind(3, relationKey.getRelationName());

                    if (statement.step()) {
                      ret = statement.columnInt(0);
                    }
                    statement.dispose();
                    statement = null;
                  } catch (final SQLiteException e) {
                    throw new CatalogException(e);
                  }
                  return (ret == 0) ? false : true;
                }
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Checking whether the relation has an is_persistent status
   *
   * @param relationKey the relation to check is_persistent status.
   * @return a boolean whether the relation is in a is_persistent status.
   * @throws CatalogException if there is an error.
   */
  public Boolean isPersistentRelation(final RelationKey relationKey) throws CatalogException {
    if (isClosed) {
      throw new CatalogException("Catalog is closed.");
    }
    try {
      return queue
          .execute(
              new SQLiteJob<Boolean>() {
                @Override
                protected Boolean job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  int ret = 0;
                  try {
                    SQLiteStatement statement =
                        sqliteConnection.prepare(
                            "SELECT is_persistent FROM relations WHERE user_name=? AND program_name=? AND relation_name=?;");
                    statement.bind(1, relationKey.getUserName());
                    statement.bind(2, relationKey.getProgramName());
                    statement.bind(3, relationKey.getRelationName());

                    if (statement.step()) {
                      ret = statement.columnInt(0);
                    }
                    statement.dispose();
                    statement = null;
                  } catch (final SQLiteException e) {
                    throw new CatalogException(e);
                  }
                  return (ret == 0) ? false : true;
                }
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }

  /**
   * Run a query on the catalog.
   *
   * @param queryString a SQL query on the catalog
   * @param outputSchema the schema of the query result
   * @return a tuple iterator over the result
   * @throws CatalogException if there is an error.
   */
  public Iterator<TupleBatch> tupleBatchIteratorFromQuery(
      final String queryString, final Schema outputSchema) throws CatalogException {
    try {
      return queue
          .execute(
              new SQLiteJob<Iterator<TupleBatch>>() {
                @Override
                protected Iterator<TupleBatch> job(final SQLiteConnection sqliteConnection)
                    throws CatalogException, SQLiteException {
                  SQLiteStatement statement = sqliteConnection.prepare(queryString);
                  List<TupleBatch> tuples = Lists.newLinkedList();
                  Iterators.addAll(
                      tuples,
                      new SQLiteTupleBatchIterator(statement, sqliteConnection, outputSchema));
                  return tuples.iterator();
                }
              })
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new CatalogException(e);
    }
  }
}
