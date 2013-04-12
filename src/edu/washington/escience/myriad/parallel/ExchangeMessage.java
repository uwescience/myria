package edu.washington.escience.myriad.parallel;

/**
 * All the messages get put into InputBuffers should be of this type.
 * 
 * @param <DATA> actual data type.
 * */
public interface ExchangeMessage<DATA> {

  /**
   * From which source the message is sent.
   * 
   * @return source IPC ID.
   * */
  int getSourceIPCID();

  /**
   * @return the actual data object.
   * */
  DATA getData();

}
