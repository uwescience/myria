package edu.washington.escience.myria.memorydb;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.proto.DataProto.DataMessage;

/**
 * A memory table. It stores {@link DataMessage} instead of {@link TupleBatch} to minimize java object memory overhead.
 * 
 * The table is append only.
 * */
public final class TupleBatchTable extends MemoryTable {

  /**
   * data buffer.
   * */
  private final List<TupleBatch> dataBuffer;

  /**
   * @param tb the TupleBatch to insert
   * */
  @Override
  public void addInner(final TupleBatch tb) {
    dataBuffer.add(tb);
  }

  /**
   * @param owner the owner {@MemoryStore} of the memory table.
   * @param schema the schema of the memory table.
   * */
  public TupleBatchTable(final MemoryStore owner, final Schema schema) {
    super(owner, schema);
    dataBuffer = new LinkedList<TupleBatch>();
  }

  /**
   * @return A {@link TupleBatch} iterator over the whole memory table.
   * */
  @Override
  public Iterator<TupleBatch> iterator() {
    return new TBIterator();
  }

  /**
   * TupleBatch Iterator.
   * */
  private class TBIterator implements Iterator<TupleBatch> {
    /**
     * storage iterator.
     * */
    private final Iterator<TupleBatch> it;

    /**
     * .
     * */
    TBIterator() {
      it = dataBuffer.iterator();
    }

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public TupleBatch next() {
      return it.next();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Read only");
    }
  }

}
