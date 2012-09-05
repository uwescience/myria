package edu.washington.escience.myriad.table;

import edu.washington.escience.myriad.parallel.DbException;

public interface DbAppendWriter extends DbTable {
  public void append(_TupleBatch tb) throws DbException;
}
