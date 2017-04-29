/**
 *
 */
package edu.washington.escience.myria.operator;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.accessmethod.AccessMethod;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;
import edu.washington.escience.myria.io.DataSource;
import edu.washington.escience.myria.operator.network.distribute.DistributeFunction;
import edu.washington.escience.myria.operator.network.distribute.HashDistributeFunction;
import edu.washington.escience.myria.parallel.RelationWriteMetadata;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * Writes a partition in native binary format directly from an InputStream.
 */
public class DbDirectInsert extends Operator implements DbWriter {
  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The logger for this class. */
  private static final Logger LOGGER = LoggerFactory.getLogger(DbDirectInsert.class);
  /** The connection to the database. */
  private AccessMethod accessMethod;
  /** The stream containing the input tuples. */
  private InputStream inputStream;
  /** The information for the database connection. */
  private ConnectionInfo connectionInfo;
  /** The source of the input tuples. */
  private final DataSource dataSource;
  /** The name of the table the tuples should be inserted into. */
  private final RelationKey relationKey;
  /** The schema of the table the tuples should be inserted into. */
  private final Schema schema;
  /** The DistributeFunction used to distribute the table across workers. */
  private final DistributeFunction distributeFunction;

  /**
   * Constructs an insertion operator to store the tuples from the specified InputStream into the
   * specified database. If the table already exists, the constructor will fail.
   *
   * @param connectionInfo connection parameters for the database containing the destination table.
   * @param dataSource the source of the tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param schema the schema of the table the tuples should be inserted into.
   * @param distributeFunction the DistributeFunction of the relation containing this table's
   *        partition.
   */
  public DbDirectInsert(
      @Nullable final ConnectionInfo connectionInfo,
      @Nonnull final DataSource dataSource,
      @Nonnull final RelationKey relationKey,
      @Nonnull final Schema schema,
      @Nonnull final DistributeFunction distributeFunction) {
    Objects.requireNonNull(dataSource, "dataSource");
    Objects.requireNonNull(relationKey, "relationKey");
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(distributeFunction, "distributeFunction");
    Preconditions.checkArgument(
        distributeFunction instanceof HashDistributeFunction,
        "DbDirectInsert requires a hash-partitioned relation");
    this.connectionInfo = connectionInfo;
    this.dataSource = dataSource;
    this.relationKey = relationKey;
    this.schema = schema;
    this.distributeFunction = distributeFunction;
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    try {
      /* retrieve connection information from the environment variables, if not already set */
      if (connectionInfo == null && execEnvVars != null) {
        connectionInfo =
            (ConnectionInfo) execEnvVars.get(MyriaConstants.EXEC_ENV_VAR_DATABASE_CONN_INFO);
      }
      if (connectionInfo == null) {
        throw new DbException("Unable to instantiate DbInsert: connection information unknown");
      }
      inputStream = dataSource.getInputStream();
      /* open the database connection */
      accessMethod = AccessMethod.of(connectionInfo.getDbms(), connectionInfo, false);
      /* Create the table */
      accessMethod.createTableIfNotExists(relationKey, getSchema());
      /* Populate the table */
      accessMethod.insertFromStream(relationKey, inputStream);
    } catch (final IOException e) {
      LOGGER.error(e.getMessage(), e);
      throw new DbException(e);
    }
  }

  @Override
  public void cleanup() {
    try {
      if (accessMethod != null) {
        accessMethod.close();
      }
      try {
        if (inputStream != null) {
          inputStream.close();
        }
      } catch (final IOException e) {
        LOGGER.warn(e.getMessage(), e);
      }
    } catch (final DbException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public final Schema generateSchema() {
    return schema;
  }

  @Override
  public final Operator[] getChildren() {
    return new Operator[] {};
  }

  @Override
  public Map<RelationKey, RelationWriteMetadata> writeSet() {
    return ImmutableMap.of(
        relationKey,
        new RelationWriteMetadata(relationKey, getSchema(), false, false, distributeFunction));
  }

  @Override
  protected TupleBatch fetchNextReady() throws Exception {
    return null;
  }

  @Override
  public void setChildren(final Operator[] children) {}
}
