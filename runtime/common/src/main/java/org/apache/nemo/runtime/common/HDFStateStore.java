package org.apache.nemo.runtime.common;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nemo.offloading.common.StateStore;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class HDFStateStore implements StateStore {
  private static final Logger LOG = LoggerFactory.getLogger(HDFStateStore.class.getName());

  private final Configuration conf = HDFSUtils.CONF;

  @Inject
  public HDFStateStore() throws IOException {
  }

  private final Map<String, Object> taskLock = new ConcurrentHashMap<>();

  @Override
  public InputStream getStateStream(String taskId) {
    taskLock.putIfAbsent(taskId, new Object());

    synchronized (taskLock.get(taskId)) {
      final Path path = new Path(HDFSUtils.STATE_PATH + "/" + taskId);
      try {
        final FileSystem fileSystem = path.getFileSystem(conf);
        return fileSystem.open(path);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public byte[] getBytes(String taskId) {
    taskLock.putIfAbsent(taskId, new Object());

    synchronized (taskLock.get(taskId)) {
      final Path path = new Path(HDFSUtils.STATE_PATH + "/" + taskId);
      try {
        final FileSystem fileSystem = path.getFileSystem(conf);
        final long len = fileSystem.getFileStatus(path).getLen();
        final byte[] bytes = new byte[(int) len];
        fileSystem.open(path).readFully(bytes);
        return bytes;
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public OutputStream getOutputStream(String taskId) {
    taskLock.putIfAbsent(taskId, new Object());

    synchronized (taskLock.get(taskId)) {
      final Path path = new Path(HDFSUtils.STATE_PATH + "/" + taskId);
      try {
        final FileSystem fileSystem = path.getFileSystem(conf);

        if (fileSystem.exists(path)) {
          LOG.info("Task state " + taskId + " already exist.. remove and rewrite");
          fileSystem.delete(path, true);
        }

        final FSDataOutputStream os = fileSystem.create(path);
        return new OutputStream() {
          @Override
          public void write(int b) throws IOException {
            os.write(b);
          }
          @Override
          public void close() {
            try {
              os.hflush();
              os.close();
            } catch (IOException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          }
        };
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void put(String taskId, byte[] bytes) {
    taskLock.putIfAbsent(taskId, new Object());

    LOG.info("Storing task state " + taskId + " into HDFS");
    synchronized (taskLock.get(taskId)) {
      final Path path = new Path(HDFSUtils.STATE_PATH + "/" + taskId);
      try {
        final FileSystem fileSystem = path.getFileSystem(conf);

        if (fileSystem.exists(path)) {
          LOG.info("Task state " + taskId + " already exist.. remove and rewrite");
          fileSystem.delete(path, true);
        }

        final FSDataOutputStream out = fileSystem.create(path);
        out.write(bytes);
        out.hflush();
        out.close();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public boolean containsState(String taskId) {
    taskLock.putIfAbsent(taskId, new Object());

    synchronized (taskLock.get(taskId)) {
      final Path path = new Path(HDFSUtils.STATE_PATH + "/" + taskId);
      try {
        final FileSystem fileSystem = path.getFileSystem(conf);
        return fileSystem.exists(path);
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void close() {

  }

  @Override
  public int getPort() {
    return 0;
  }
}