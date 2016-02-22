package edu.washington.escience.myria.operator.mmap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

// https://github.com/mitallast/netty-queue/blob/master/src/main/java/org/mitallast/queue/common/mmap/MemoryMappedFileBuffer.java
public class MemoryMappedFileBuffer implements Closeable {
    private final File file;
    private final RandomAccessFile randomAccessFile;
    private final MappedByteBuffer mappedByteBuffer;
    private final ByteBuf buffer;
    private volatile boolean closed = false;

    public MemoryMappedFileBuffer(File file, long size) throws IOException {
        this(file, 0, size);
    }

    public MemoryMappedFileBuffer(File file, long offset, long size) throws IOException {
        this.file = file;
        this.randomAccessFile = new RandomAccessFile(file, "rw");
        FileChannel channel = randomAccessFile.getChannel();

        mappedByteBuffer = channel.map(FileChannel.MapMode.READ_WRITE, offset, size);
        mappedByteBuffer.load();
        mappedByteBuffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);

        buffer = Unpooled.wrappedBuffer(mappedByteBuffer);
    }

    public File file() {
        return file;
    }

    public RandomAccessFile randomAccessFile() {
        return randomAccessFile;
    }

    public ByteBuf buffer() {
        return buffer;
    }

    public void flush() throws IOException {
        synchronized (this) {
            if (closed) {
                return;
            }
            mappedByteBuffer.force();
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            flush();
            if (closed) {
                return;
            }
            closed = true;
            MappedByteBufferCleaner.clean(mappedByteBuffer);
            randomAccessFile.close();
        }
    }
}