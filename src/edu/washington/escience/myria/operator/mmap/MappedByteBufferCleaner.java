package edu.washington.escience.myria.operator.mmap;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

//https://raw.githubusercontent.com/mitallast/netty-queue/master/src/main/java/org/mitallast/queue/common/mmap/MappedByteBufferCleaner.java
class MappedByteBufferCleaner {
    public static final boolean cleanSupported;
    private static final Method directCleaner;
    private static final Method directCleanerClean;

    static {
        Method directBufferCleanerX = null;
        Method directBufferCleanerCleanX = null;
        boolean v;
        try {
            directBufferCleanerX = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
            directBufferCleanerX.setAccessible(true);
            directBufferCleanerCleanX = Class.forName("sun.misc.Cleaner").getMethod("clean");
            directBufferCleanerCleanX.setAccessible(true);
            v = true;
        } catch (Exception e) {
            v = false;
        }
        cleanSupported = v;
        directCleaner = directBufferCleanerX;
        directCleanerClean = directBufferCleanerCleanX;
    }

    public static void clean(MappedByteBuffer buffer) {
        if (buffer == null) return;
        if (cleanSupported && buffer.isDirect()) {
            try {
                Object cleaner = directCleaner.invoke(buffer);
                directCleanerClean.invoke(cleaner);
            } catch (Exception e) {
                // silently ignore exception
            }
        }
    }
}