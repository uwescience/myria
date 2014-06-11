package edu.washington.escience.myria.util;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A implementation for the {@link Attachmentable} interface.
 * */
public abstract class AttachmentableAdapter implements Attachmentable {

  /**
   * Attachment holder.
   * */
  private final AtomicReference<Object> att = new AtomicReference<Object>();

  @Override
  public final Object getAttachment() {
    return att.get();
  }

  @Override
  public final Object setAttachment(final Object attachment) {
    return att.getAndSet(attachment);
  }
}
