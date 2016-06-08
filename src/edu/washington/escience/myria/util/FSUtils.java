package edu.washington.escience.myria.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.security.AccessControlException;

/**
 * Filesystem util methods.
 * */
public final class FSUtils {
  /** A short amount of time to sleep waiting for filesystem events. */
  public static final int SHORT_SLEEP_MILLIS = 100;
  /** Block size for file copying. */
  public static final int BLOCK_SIZE_BYTES = 1024;

  /**
   * Delete the pathToDirectory. Return only if the directory is actually get deleted on the disk.
   *
   * @param pathToDirectory the directory to be deleted.
   */
  public static void blockingDeleteDirectory(final String pathToDirectory) {
    final File testBaseFolderF = new File(pathToDirectory);
    try {
      FSUtils.deleteFileFolder(testBaseFolderF);
    } catch (final IOException e) {
      e.printStackTrace();
    }
    boolean finishClean = false;
    while (!finishClean) {
      finishClean = !testBaseFolderF.exists();
      if (!finishClean) {
        try {
          Thread.sleep(SHORT_SLEEP_MILLIS);
        } catch (final InterruptedException e) {
          e.printStackTrace();
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /**
   * @param source the source file or folder
   * @param dest the dest file or folder
   * @param override if to override if the dest exists.
   * @throws IOException if any error occurs.
   */
  public static void copyFileFolder(final File source, final File dest, final boolean override)
      throws IOException {
    if (dest.exists()) {
      if (!override) {
        return;
      } else {
        deleteFileFolder(dest);
      }
    }

    if (source.isDirectory()) {
      dest.mkdirs();
      final File[] children = source.listFiles();
      for (final File child : children) {
        copyFileFolder(child, new File(dest.getAbsolutePath() + "/" + child.getName()), override);
      }
    } else {
      InputStream in = null;
      OutputStream out = null;
      try {
        in = new FileInputStream(source);
        out = new FileOutputStream(dest);

        // Transfer bytes from in to out
        final byte[] buf = new byte[BLOCK_SIZE_BYTES];
        int len;
        while ((len = in.read(buf)) > 0) {
          out.write(buf, 0, len);
        }
      } finally {
        if (in != null) {
          in.close();
        }
        if (out != null) {
          out.close();
        }
      }
    }
  }

  /**
   * @param f the file or folder to be deleted.
   * @throws IOException if any error occurs.
   * */
  public static void deleteFileFolder(final File f) throws IOException {
    if (!f.exists()) {
      return;
    }
    if (f.isDirectory()) {
      for (final File c : f.listFiles()) {
        deleteFileFolder(c);
      }
    }
    if (!f.delete()) {
      throw new FileNotFoundException("Failed to delete file: " + f);
    }
  }

  /**
   * Write the content to the file.
   *
   * @param f the file.
   * @param content the content.
   * @throws IOException if any error occurs.
   * */
  public static void writeFile(final File f, final String content) throws IOException {
    final FileOutputStream o = new FileOutputStream(f);
    o.write(content.getBytes());
    o.close();
  }

  /**
   * Reset the file length of a file.
   *
   * @param f the file.
   * @param desired the file length desired.
   * @throws IOException if any error occurs.
   * */
  public static void resetFileSize(final File f, final long desired) throws IOException {
    boolean ok = f.length() == desired;
    if (!ok) {
      RandomAccessFile raf = null;
      try {
        raf = new RandomAccessFile(f, "rws");
        raf.setLength(desired);
      } finally {
        if (raf != null) {
          raf.close();
        }
      }
    }
  }

  /**
   * Check whether the specified filepath is a file and is readable.
   *
   * @param filepath the path to the file.
   * @throws FileNotFoundException if the specified filepath does not exist or is not a file.
   */
  public static void checkFileReadable(final String filepath) throws FileNotFoundException {
    File f = new File(filepath);
    if (!f.exists()) {
      throw new FileNotFoundException(filepath + " could not be found");
    }
    if (!f.isFile()) {
      throw new FileNotFoundException(filepath + " is not a file");
    }
    if (!f.canRead()) {
      throw new AccessControlException(filepath + " could not be read");
    }
  }

  /**
   * util classes are not instantiable.
   * */
  private FSUtils() {}
}
