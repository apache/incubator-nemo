package org.apache.nemo.runtime.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class HDFSUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HDFSUtils.class.getName());

  public static final Configuration CONF = new Configuration();
  public static final String STATE_PATH = "hdfs://hdfs-master:9000/sponge_state";

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
