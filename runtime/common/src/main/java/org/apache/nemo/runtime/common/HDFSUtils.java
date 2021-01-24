package org.apache.nemo.runtime.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public final class HDFSUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSUtils.class.getName());

  public static final Configuration CONF = new Configuration();
  public static final String STATE_PATH = "hdfs://hdfs-master:9000/sponge_state";
  public static final String PLAN_PATH = "hdfs://hdfs-master:9000/plan_dag";

  private static final Long timestamp = System.currentTimeMillis();

  public static void createPlanDir(final String jobId,
                                   final String planId,
                                   final int dagLogFileIndex,
                                   final String suffix,
                                   final String plan) throws IOException {
    final Path path = new Path(PLAN_PATH + "/" + jobId + "-" + timestamp
    + "/" + planId + "-" + dagLogFileIndex + "-" + suffix + ".json");
    LOG.info("Creating plan dir " + path.getName());
    final FileSystem fileSystem = path.getFileSystem(CONF);
    try {
      if (!fileSystem.exists(path)) {
        fileSystem.mkdirs(path);
      }

      final OutputStream out = new BufferedOutputStream(fileSystem.create(path));
      out.write(plan.getBytes("UTF-8"));
      out.close();

    } catch (IOException e) {
      // Ignore this exception, if there is a problem it'll fail when trying to read or write.
      LOG.warn("Error while trying to set permission: ", e);
    }
  }

  public static void createStateDirIfNotExistsAndDelete() throws IOException {
    LOG.info("Creating sponge state dir");
    final Path path = new Path(STATE_PATH);
    final FileSystem fileSystem = path.getFileSystem(CONF);
    try {
      if (!fileSystem.exists(path)) {
        fileSystem.mkdirs(path);
      } else {
        fileSystem.delete(path, true);
        fileSystem.mkdirs(path);
      }
    } catch (IOException e) {
      // Ignore this exception, if there is a problem it'll fail when trying to read or write.
      LOG.warn("Error while trying to set permission: ", e);
    }
  }
}
