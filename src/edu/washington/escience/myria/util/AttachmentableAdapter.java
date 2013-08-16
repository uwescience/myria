package edu.washington.escience.myria.util;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A implementation for the Attachmemtable interface.
 * */
public abstract class AttachmentableAdapter {

  /**
   * Attachment holder.
   * */
  private final AtomicReference<Object> att = new AtomicReference<Object>();

  /**
   * @return attachment.
   * */
  public final Object getAttachment() {
    return att.get();
  }

  /**
   * Set attachment to the new value and return old value.
   * 
   * @return the old value.
   * @param attachment the new attachment
   * */
  public final Object setAttachment(final Object attachment) {
    return att.getAndSet(attachment);
  }

  /**
   * Set attachment to the value only if currently no attachment is set.
   * 
   * @return if the attachment is already set, return the old value, otherwise return null.
   * @param attachment the attachment to set
   * */
  public final Object setAttachmentIfAbsent(final Object attachment) {
    while (true) {
      Object oldValue = att.get();
      if (oldValue != null) {
        return oldValue;
      }
      if (att.compareAndSet(null, attachment)) {
        return null;
      }
    }
  }
}
