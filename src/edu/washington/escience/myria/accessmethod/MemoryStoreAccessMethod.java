package edu.washington.escience.myria.accessmethod;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

import javax.annotation.concurrent.NotThreadSafe;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.memorydb.MemTableAppender;
import edu.washington.escience.myria.memorydb.MemoryStore;
import edu.washington.escience.myria.memorydb.MemoryStoreInfo;

/**
 * Access method for MemoryDB. Exposes data as TupleBatches.
 * 
 */
@NotThreadSafe
public final class MemoryStoreAccessMethod extends AccessMethod {

  /** The connection information. **/
  private MemoryStoreInfo memoryDBInfo;
  /** Flag that identifies the connection type (read-only or not). **/
  private boolean readOnly;
  /**
   * Memory store instance.
   * */
  private final MemoryStore memoryStore;
  /**
   * Memory table insert handles.
   * */
  private final HashMap<RelationKey, MemTableAppender> mta;

  /**
   * The constructor. Creates an object and connects with the database
   * 
   * @param memoryStore the memory store
   * @param memoryStoreInfo connection information
   * @param readOnly whether read-only connection or not
   * @throws DbException if there is an error making the connection.
   */
  public MemoryStoreAccessMethod(final MemoryStore memoryStore, final MemoryStoreInfo memoryStoreInfo,
      final boolean readOnly) throws DbException {
    this.memoryStore = memoryStore;
    mta = new HashMap<RelationKey, MemTableAppender>();
    connect(memoryStoreInfo, readOnly);
  }

  @Override
  public void connect(final ConnectionInfo connectionInfo, final Boolean readOnly) throws DbException {
    Objects.requireNonNull(connectionInfo);
    memoryDBInfo = (MemoryStoreInfo) connectionInfo;
    this.readOnly = readOnly;
  }

  @Override
  public void setReadOnly(final Boolean readOnly) throws DbException {
    Objects.requireNonNull(memoryDBInfo);
    this.readOnly = readOnly;
  }

  @Override
  public void tupleBatchInsert(final RelationKey relationKey, final Schema schema, final TupleBatch tupleBatch)
      throws DbException {
    Objects.requireNonNull(memoryDBInfo);
    MemTableAppender m = mta.get(relationKey);
    if (m == null) {
      throw new DbException("Table " + relationKey.toString(MyriaConstants.STORAGE_SYSTEM_MEMORYSTORE)
          + " does not exist");
    }
    m.add(tupleBatch);
  }

  @Override
  public Iterator<TupleBatch> tupleBatchIteratorFromQuery(final String queryString, final Schema schema)
      throws DbException {
    throw new DbException("Query iterator is not supported.");
  }

  /**
   * Scan a memory table.
   * 
   * @param key the table to scan
   * @return iterator over table contents.
   * @throws DbException if error occurs or the table does not exist.
   * */
  public Iterator<TupleBatch> tupleBatchIterator(final RelationKey key) throws DbException {
    Iterator<TupleBatch> r = memoryStore.getReader(key);
    if (r == null) {
      throw new DbException("Table does not exist");
    }
    return r;
  }

  @Override
  public void execute(final String ddlCommand) throws DbException {
    throw new DbException("DDL commands in memroy store is not supported.");
  }

  @Override
  public void close() throws DbException {
    for (Entry<RelationKey, MemTableAppender> m : mta.entrySet()) {
      m.getValue().close();
    }
  }

  @Override
  public String insertStatementFromSchema(final Schema schema, final RelationKey relationKey) {
    return null;
  }

  @Override
  public String createIfNotExistsStatementFromSchema(final Schema schema, final RelationKey relationKey) {
    return null;
  }

  @Override
  public void createTableIfNotExists(final RelationKey relationKey, final Schema schema) throws DbException {
    if (readOnly) {
      throw new DbException("Read only.");
    }
    try {
      mta.put(relationKey, memoryStore.createTable(relationKey, schema, memoryDBInfo.getMemoryTableImpl()));
    } catch (DbException e) {
      // table exists.
      return;
    }
  }

  @Override
  public void dropAndRenameTables(final RelationKey oldRelation, final RelationKey newRelation) throws DbException {
    memoryStore.dropAndRenameTable(oldRelation, newRelation);
  }

  @Override
  public void renameIndexes(final RelationKey oldRelation, final RelationKey newRelation,
      final List<List<IndexRef>> indexes) throws DbException {
    /* Do nothing -- rather than renaming the right way, we create every index with a different unique name. */
    return;
  }

  @Override
  public void dropTableIfExists(final RelationKey relationKey) throws DbException {
    memoryStore.dropTable(relationKey);
  }

  @Override
  public void createIndexes(final RelationKey relationKey, final Schema schema, final List<List<IndexRef>> indexes)
      throws DbException {
    if (indexes != null && indexes.size() > 0) {
      throw new DbException("Index is not supported in memory store.");
    }
  }
}
