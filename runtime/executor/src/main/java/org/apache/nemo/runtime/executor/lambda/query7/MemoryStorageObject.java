package org.apache.nemo.runtime.executor.lambda.query7;

import org.apache.nemo.common.DirectByteArrayOutputStream;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.executor.lambda.StorageObjectFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public final class MemoryStorageObject implements StorageObjectFactory.StorageObject {
    private final EncoderFactory.Encoder encoder;
    public final DirectByteArrayOutputStream outputStream;
    public final String fname;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private boolean finished = false;
    private final int partition;
    private final String prefix;
    int cnt = 0;
    public MemoryStorageObject(final String prefix, final int partition,
                               final byte[] encodedDecoderFactory,
                               final EncoderFactory encoderFactory) {
      this.prefix = prefix;
      this.fname = prefix + "-" + partition;
      this.partition = partition;
      this.outputStream = new DirectByteArrayOutputStream();
      try {
        outputStream.write(encodedDecoderFactory);
        this.encoder = encoderFactory.create(outputStream);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void encode(Object object) {
      try {
        cnt += 1;
        encoder.encode(object);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) {
        try {
          outputStream.close();
          finished = true;
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    }

    @Override
    public int getPartition() {
      return partition;
    }

    @Override
    public String getPrefix() {
      return prefix;
    }

    @Override
    public String toString() {
      return fname;
    }
  }
