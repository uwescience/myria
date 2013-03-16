package edu.washington.escience.myriad.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

public class LocalClassLoader extends ClassLoader {

  static class ClassPathItem {

    ClassPathItem(File binFolder) throws MalformedURLException {
      this.binFolder = binFolder;
    }

    ClassPathItem(JarResources jar) {
      this.jar = jar;
    }

    boolean isJar() {
      return jar != null;
    }

    File binFolder = null;
    JarResources jar = null;
  }

  protected ArrayList<ClassPathItem> classpaths = new ArrayList<ClassPathItem>();
  protected ClassLoader parent;

  /**
   * load the classes using the classpaths first, if not found, use the parent
   * */
  public LocalClassLoader(String[] classpaths, ClassLoader parent) {
    super(null);
    this.parent = parent;
    try {
      for (String classpath : classpaths) {
        if (classpath.endsWith(".jar")) {
          this.classpaths.add(new ClassPathItem(new JarResources(classpath)));
        } else {
          File libOrBinD;
          if (classpath.equals("*")) {
            libOrBinD = new File(".");
          } else if (classpath.endsWith("/*")) {
            libOrBinD = new File(classpath.substring(0, classpath.length() - 1));
          } else {
            libOrBinD = new File(classpath);
          }
          if (libOrBinD.isDirectory()) {
            if (classpath.endsWith("*")) {
              File[] subs = libOrBinD.listFiles();
              if (subs != null) {
                for (File f : subs) {
                  if (f.getName().toLowerCase().endsWith(".jar")) {
                    this.classpaths.add(new ClassPathItem(new JarResources(f.getAbsolutePath())));
                  }
                }
              }
            } else {
              this.classpaths.add(new ClassPathItem(libOrBinD));
            }
          }
        }
      }

    } catch (Exception e) {
      throw new IllegalArgumentException();
    }
  }

  @Override
  protected Class<?> findClass(String name) {
    String resourceName = name.replace('.', '/') + ".class";
    byte buf[] = null;
    Class<?> cl;

    for (ClassPathItem cp : classpaths) {
      if (cp.isJar()) {
        buf = cp.jar.getResource(resourceName);
      } else {
        try {
          buf = readFileAll(cp.binFolder.getAbsolutePath() + "/" + resourceName).array();
        } catch (Throwable e) {
        }
      }
      if (buf != null) {
        cl = defineClass(name, buf, 0, buf.length);
        return cl;
      }
    }
    try {
      if (parent != null) {
        return parent.loadClass(name);
      }
    } catch (ClassNotFoundException e) {
    }
    return null;
  }

  public static ByteBuffer readFileAll(String filePath) throws IOException {
    File f = new File(filePath);
    if (!f.isFile()) {
      return null;
    }

    FileInputStream in = new FileInputStream(f);
    FileChannel fc = in.getChannel();
    ByteBuffer result = ByteBuffer.allocate((int) fc.size());
    fc.read(result);
    return result;
  }
}
