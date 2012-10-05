package edu.washington.escience.myriad.table;

import edu.washington.escience.myriad.DbException;

public interface DbAppendWriter extends DbTable {
  public void append(_TupleBatch tb) throws DbException;
}
