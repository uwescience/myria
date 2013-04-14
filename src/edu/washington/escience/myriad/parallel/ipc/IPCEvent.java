package edu.washington.escience.myriad.parallel.ipc;

/**
 * An @{link IPCEvent} represents an event from the IPC layer. Each {@link IPCEvent} can have an attachment so that the
 * event source can provide detailed information to the event listeners.
 * 
 * @param <ATT> the type of attachment.
 * */
public interface IPCEvent<ATT> {

  /**
   * Any attachment for IOEventListeners to get information from the event.
   * 
   * @return attachment object.
   * */
  ATT getAttachment();

}
