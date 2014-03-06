package edu.washington.escience.myria.memorydb;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import net.jcip.annotations.Immutable;

import com.google.protobuf.InvalidProtocolBufferException;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.TupleBatch;
import edu.washington.escience.myria.proto.DataProto.DataMessage;
import edu.washington.escience.myria.util.IPCUtils;

/**
 * A memory table. It stores {@link DataMessage} instead of {@link TupleBatch} to minimize java object memory overhead.
 * 
 * The table is readonly.
 * */
@Immutable
public final class ProtobufTable extends MemoryTable {

  /**
   * Storage.
   * */
  private final List<byte[]> dataStore;

  /**
   * Simple statistics, total size.
   * */
  private long totalSizeInBytes;

  /**
   * @param owner the owner {@MemoryStore} of the memory table.
   * @param schema the schema of the memory table.
   * */
  public ProtobufTable(final MemoryStore owner, final Schema schema) {
    super(owner, schema);
    dataStore = new LinkedList<byte[]>();
  }

  /**
   * @return num tuples.
   * */
  public long getTotalSize() {
    return totalSizeInBytes;
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
    private final Iterator<byte[]> it;

    /**
     * .
     * */
    TBIterator() {
      it = dataStore.iterator();
    }

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public TupleBatch next() {
      byte[] stb = it.next();
      if (stb == null) {
        return null;
      }
      try {
        return IPCUtils.tmToTupleBatch(DataMessage.parseFrom(stb), getSchema());
      } catch (InvalidProtocolBufferException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Read and append only");
    }
  }

  @Override
  protected void addInner(final TupleBatch tb) {
    byte[] serialized = tb.toTransportMessage().getDataMessage().toByteArray();
    totalSizeInBytes += serialized.length;
    dataStore.add(serialized);
  }
}
