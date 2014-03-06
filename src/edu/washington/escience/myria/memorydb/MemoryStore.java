package edu.washington.escience.myria.memorydb;

import java.util.HashMap;
import java.util.Iterator;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Iterators;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.RelationKey;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.accessmethod.MemoryStoreAccessMethod;

/**
 * A simple implementation of in-memory relation store. Data table is created once and then read only thereafter.
 * */
@ThreadSafe
public final class MemoryStore {

  /**
   * A placeholder table is a table that gets created right after the table is asked to get created but before the
   * contents of the table is ready. When the actual contents of the new table are prepared, the placeholder table will
   * get replaced by the actual table.
   * */
  private static class PlaceholderTable extends MemoryTable {

    /**
     * @param owner the owner
     * @param schema schema.
     */
    public PlaceholderTable(final MemoryStore owner, final Schema schema) {
      super(owner, schema);
    }

    @Override
    public Iterator<TupleBatch> iterator() {
      return Iterators.<TupleBatch> emptyIterator();
    }

    @Override
    protected void addInner(final TupleBatch tb) {
      throw new UnsupportedOperationException("Read only");
    }
  }

  /**
   * table name to table data.
   * */
  private final HashMap<RelationKey, MemoryTable> name2Table;

  /**
   * Memory store.
   * */
  public MemoryStore() {
    name2Table = new HashMap<RelationKey, MemoryTable>();
  }

  /**
   * find a table by the ID or name.
   * 
   * @param tableID table ID
   * @return the content TupleBatch Iterator, null if the table does not exist.
   * */
  public synchronized Iterator<TupleBatch> getReader(final RelationKey tableID) {
    MemoryTable mt = name2Table.get(tableID);
    if (mt == null) {
      return null;
    }
    return mt.iterator();
  }

  /**
   * Create a table if it's not in the memory store yet. Otherwise, throw an exception.
   * 
   * @param tableID the talbe ID or name
   * @param schema the schema of the table
   * @param cls the choice of {@link MemoryTable} implementation
   * @return the insert object for data insert.
   * @throws DbException if the table already exists.
   * */
  public synchronized MemTableAppender createTable(final RelationKey tableID, final Schema schema,
      final Class<? extends MemoryTable> cls) throws DbException {

    if (!name2Table.containsKey(tableID)) {
      MemoryTable mt;
      try {
        mt = cls.getConstructor(MemoryStore.class, Schema.class).newInstance(this, schema);
      } catch (Exception e) {
        throw new DbException(e);
      }
      MemoryTable placeholder = new PlaceholderTable(this, schema);
      name2Table.put(tableID, placeholder);
      return new MemTableAppender(this, tableID, mt, placeholder);
    }
    throw new DbException("Table " + tableID.toString(MyriaConstants.STORAGE_SYSTEM_MEMORYSTORE) + " already exists");
  }

  /**
   * Callback by {@link MemTableAppender} when the data insertion completes.
   * 
   * @param appender the data appender.
   * */
  synchronized void completeInsert(final MemTableAppender appender) {
    RelationKey tableName = appender.getRelation();
    MemoryTable p = appender.getPlaceholder();
    MemoryTable tb = name2Table.get(tableName);
    if (tb == p) {
      name2Table.put(tableName, appender.getTable());
      return;
    }
    throw new java.util.ConcurrentModificationException(tableName.toString(MyriaConstants.STORAGE_SYSTEM_MEMORYSTORE)
        + " gets dropped during data insertion.");
  }

  /**
   * Semantic: if the table gets dropped is already being opened for reading/writing, the reader can still read/write
   * stuff from/to it successfully. But new readers/writers cannot be created.
   * 
   * @param tableID tableID.
   * @return if the drop operation actually dropped a table
   * */
  public synchronized boolean dropTable(final RelationKey tableID) {
    return name2Table.remove(tableID) != null;
  }

  /**
   * @param tableID the table to rename
   * @param newID the new name
   * @throws DbException if table with the new name already exists or the table to be renamed does not exist.
   * */
  public synchronized void renameTable(final RelationKey tableID, final RelationKey newID) throws DbException {
    MemoryTable table = name2Table.get(tableID);
    if (table == null) {
      throw new DbException("Table " + newID.toString(MyriaConstants.STORAGE_SYSTEM_MEMORYSTORE) + " does not exist.");
    }
    if (name2Table.get(newID) != null) {
      throw new DbException("A table with the new relation name "
          + newID.toString(MyriaConstants.STORAGE_SYSTEM_MEMORYSTORE) + " already exists.");
    }
    name2Table.remove(tableID);
    name2Table.put(newID, table);
  }

  /**
   * Drop the toDrop table and rename the toRename table to the name of the toDrop table.
   * 
   * @param toRename the table to get renamed
   * @param toDrop the table to get dropped
   * @throws DbException if table to be renamed does not exist.
   * */
  public synchronized void dropAndRenameTable(final RelationKey toRename, final RelationKey toDrop) throws DbException {
    MemoryTable toRenameT = name2Table.get(toRename);
    if (toRenameT == null) {
      throw new DbException("Table " + toRename.toString(MyriaConstants.STORAGE_SYSTEM_MEMORYSTORE)
          + " does not exist.");
    }
    name2Table.remove(toDrop);
    name2Table.remove(toRename);
    name2Table.put(toDrop, toRenameT);
  }

  /**
   * @param key the table
   * @return the schema of the table
   * */
  public synchronized Schema getSchema(final RelationKey key) {
    MemoryTable mt = name2Table.get(key);
    if (mt == null) {
      return null;
    }
    return null;
  }

  /**
   * 
   * @param connectionInfo the connection info
   * @param readonly if the access method is read only
   * @throws DbException if any error occurs
   * @return an access method
   * */
  public MemoryStoreAccessMethod getAccessMethod(final MemoryStoreInfo connectionInfo, final boolean readonly)
      throws DbException {
    return new MemoryStoreAccessMethod(this, connectionInfo, readonly);
  }

}
