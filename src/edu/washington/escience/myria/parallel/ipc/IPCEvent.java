package edu.washington.escience.myria.parallel.ipc;

/**
 * An @{link IPCEvent} represents an event from the IPC layer. Each {@link IPCEvent} can have an attachment so that the
 * event source can provide detailed information to the event listeners.
 *
 * */
public interface IPCEvent {

  /**
   * event type.
   * */
  public final class EventType {
    /**
     * only for human readability.
     */
    private final String desc;

    /**
     * @param desc the human readable description of the type.
     * */
    public EventType(final String desc) {
      this.desc = desc;
    }

    @Override
    public String toString() {
      return "EventType(" + desc + ")";
    }
  }

  /**
   * @return the event type.
   * */
  EventType getType();

  /**
   * @return attachment.
   * */
  Object getAttachment();
}
