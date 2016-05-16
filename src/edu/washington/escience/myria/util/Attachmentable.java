package edu.washington.escience.myria.util;

/**
 * A utility interface denoting a class is able to attach an Object.
 * */
public interface Attachmentable {

  /**
   * @return attachment.
   * */
  Object getAttachment();

  /**
   * Set attachment to the new value and return old value.
   *
   * @return the old value.
   * @param attachment the new attachment
   * */
  Object setAttachment(final Object attachment);
}
