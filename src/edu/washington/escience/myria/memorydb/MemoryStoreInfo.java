package edu.washington.escience.myria.memorydb;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.accessmethod.ConnectionInfo;

/**
 * Memory connection info.
 * 
 */
public final class MemoryStoreInfo extends ConnectionInfo implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 5699133483308357526L;

  /**
   * @param implClass implementation class
   * @return a new instance
   * */
  @SuppressWarnings("unchecked")
  public static MemoryStoreInfo of(final String implClass) {
    try {
      return new MemoryStoreInfo((Class<? extends MemoryTable>) Class.forName(implClass));
    } catch (final ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * {@link MemoryTable} implementation class.
   * */
  @JsonProperty
  private final Class<? extends MemoryTable> memoryTableImpl;

  /**
   * @return the DBMS name/type
   */
  @Override
  public String getDbms() {
    return MyriaConstants.STORAGE_SYSTEM_MEMORYSTORE;
  }

  /**
   * Creates a new JdbcInfo object.
   * 
   * @param memoryTableImpl the choice of table implementation.
   */
  public MemoryStoreInfo(final Class<? extends MemoryTable> memoryTableImpl) {
    this.memoryTableImpl = memoryTableImpl;
  }

  /**
   * @return the {@link MemoryTable} implementation class.
   * */
  public Class<? extends MemoryTable> getMemoryTableImpl() {
    return memoryTableImpl;
  }

}