/*
 * Copyright (C) 2018 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nemo.examples.beam;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import javax.annotation.Nullable;
import static org.apache.beam.repackaged.beam_runners_core_java.com.google.common.base.MoreObjects.firstNonNull;

 /**
  * This class is brought from beam/examples/common/WriteOneFilePerWindow.java.
  *
  */
public final class WriteOneFilePerWindow extends PTransform<PCollection<String>, PDone> {
  // change from hourMinute to hourMinuteSecond
  private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinuteSecond();
  private String filenamePrefix;
  @Nullable
  private Integer numShards;
   public WriteOneFilePerWindow(final String filenamePrefix, final Integer numShards) {
    this.filenamePrefix = filenamePrefix;
    this.numShards = numShards;
  }
   @Override
  public PDone expand(final PCollection<String> input) {
    final ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(filenamePrefix);
    TextIO.Write write =
        TextIO.write()
            .to(new PerWindowFiles(resource))
            .withTempDirectory(resource.getCurrentDirectory())
            .withWindowedWrites();
    if (numShards != null) {
      write = write.withNumShards(numShards);
    }
    return input.apply(write);
  }
   /**
   * A {@link FilenamePolicy} produces a base file name for a write based on metadata about the data
   * being written. This always includes the shard number and the total number of shards. For
   * windowed writes, it also includes the window and pane index (a sequence number assigned to each
   * trigger firing).
   */
  public static final class PerWindowFiles extends FileBasedSink.FilenamePolicy {
     private final ResourceId baseFilename;
     PerWindowFiles(final ResourceId baseFilename) {
      this.baseFilename = baseFilename;
    }
     String filenamePrefixForWindow(final IntervalWindow window) {
      final String prefix =
          baseFilename.isDirectory() ? "" : firstNonNull(baseFilename.getFilename(), "");
      return String.format(
          "%s-%s-%s", prefix, FORMATTER.print(window.start()), FORMATTER.print(window.end()));
    }
     @Override
    public ResourceId windowedFilename(
        final int shardNumber,
        final int numShards,
        final BoundedWindow window,
        final PaneInfo paneInfo,
        final FileBasedSink.OutputFileHints outputFileHints) {
      System.out.println("Windowd file name: " + window);
      final IntervalWindow intervalWindow = (IntervalWindow) window;
      final String filename =
          String.format(
              "%s-%s-of-%s%s",
              filenamePrefixForWindow(intervalWindow),
              shardNumber,
              numShards,
              outputFileHints.getSuggestedFilenameSuffix());
      return baseFilename
          .getCurrentDirectory()
          .resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    }
     @Override
    public ResourceId unwindowedFilename(
        final int shardNumber, final int numShards, final FileBasedSink.OutputFileHints outputFileHints) {
      throw new UnsupportedOperationException("Unsupported.");
    }
  }
}
