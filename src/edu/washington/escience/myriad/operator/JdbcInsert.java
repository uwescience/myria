package edu.washington.escience.myriad.operator;

import java.util.Objects;

import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myriad.DbException;
import edu.washington.escience.myriad.RelationKey;
import edu.washington.escience.myriad.TupleBatch;
import edu.washington.escience.myriad.accessmethod.JdbcAccessMethod;
import edu.washington.escience.myriad.accessmethod.JdbcInfo;
import edu.washington.escience.myriad.util.JdbcUtils;

/**
 * An operator that inserts its tuples into a relation stored in a JDBC Database.
 * 
 * @author dhalperi
 * 
 */
public final class JdbcInsert extends RootOperator {

  /** Required for Java serialization. */
  private static final long serialVersionUID = 1L;
  /** The connection to the JDBC database. */
  private JdbcAccessMethod jdbcAccessMethod;
  /** The information for the JDBC connection. */
  private final JdbcInfo jdbcInfo;
  /** The name of the table the tuples should be inserted into. */
  private final RelationKey relationKey;
  /** Whether to overwrite an existing table or not. */
  private final boolean overwriteTable;
  /** The statement used to insert tuples into the database. */
  private String insertString;

  /**
   * Constructs an insertion operator to store the tuples from the specified child in a SQLite database in the specified
   * file. If the table does not exist, it will be created; if it does exist then old data will persist and new data
   * will be inserted.
   * 
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param jdbcInfo the parameters of the JDBC connection.
   */
  public JdbcInsert(final Operator child, final RelationKey relationKey, final JdbcInfo jdbcInfo) {
    this(child, relationKey, jdbcInfo, false);
  }

  /**
   * Constructs an insertion operator to store the tuples from the specified child in a SQLite database in the specified
   * file. If the table does not exist, it will be created. If the table exists and overwriteTable is true, the existing
   * data will be dropped from the database before the new data is inserted. If overwriteTable is false, any existing
   * data will remain and new data will be appended.
   * 
   * @param child the source of tuples to be inserted.
   * @param relationKey the key of the table the tuples should be inserted into.
   * @param jdbcInfo the parameters of the JDBC connection.
   * @param overwriteTable whether to overwrite a table that already exists.
   */
  public JdbcInsert(final Operator child, final RelationKey relationKey, final JdbcInfo jdbcInfo,
      final boolean overwriteTable) {
    super(child);
    Objects.requireNonNull(child);
    Objects.requireNonNull(relationKey);
    Objects.requireNonNull(jdbcInfo);
    this.jdbcInfo = jdbcInfo;
    this.relationKey = relationKey;
    this.overwriteTable = overwriteTable;
  }

  @Override
  public void cleanup() {
    try {
      jdbcAccessMethod.close();
    } catch (DbException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void consumeTuples(final TupleBatch tupleBatch) throws DbException {
    jdbcAccessMethod.tupleBatchInsert(insertString, tupleBatch);
  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    /* Set up the insert statement. */
    insertString = JdbcUtils.insertStatementFromSchema(getSchema(), relationKey, jdbcInfo.getDbms());
    /* open the JDBC Connection */
    jdbcAccessMethod = new JdbcAccessMethod(jdbcInfo, false);
    /* create the table */
    jdbcAccessMethod.createTable(relationKey, getSchema(), jdbcInfo.getDbms(), overwriteTable);
  }

  @Override
  protected void childEOS() throws DbException {
  }

  @Override
  protected void childEOI() throws DbException {
  }

}
