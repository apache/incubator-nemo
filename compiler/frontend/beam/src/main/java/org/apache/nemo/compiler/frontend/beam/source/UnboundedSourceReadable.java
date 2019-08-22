package org.apache.nemo.compiler.frontend.beam.source;

import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.kafka.KafkaUnboundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.nemo.common.ir.Readable;
import org.apache.nemo.common.punctuation.EmptyElement;
import org.apache.nemo.common.punctuation.TimestampAndValue;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;

/**
 * UnboundedSourceReadable class.
 * @param <O> output type.
 * @param <M> checkpoint mark type.
 */
public final class UnboundedSourceReadable<O, M extends UnboundedSource.CheckpointMark> implements Readable<Object> {
  private final UnboundedSource<O, M> unboundedSource;
  private UnboundedSource.UnboundedReader<O> reader;
  private boolean isStarted = false;
  private volatile boolean isCurrentAvailable = false;
  private volatile boolean isKafkaPolled = false;
  private volatile boolean isKafkaPolling = false;
  private boolean isFinished = false;

  private final PipelineOptions pipelineOptions;
  private final M checkpointMark;

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedSourceReadable.class.getName());

  private KafkaUnboundedReader kafkaReader;


  private ExecutorService readableService;
  /**
   * Constructor.
   * @param unboundedSource unbounded source.
   */
  public UnboundedSourceReadable(final UnboundedSource<O, M> unboundedSource) {
    this(unboundedSource, null, null);
  }

  public UnboundedSourceReadable(final UnboundedSource<O, M> unboundedSource,
                                 final PipelineOptions options,
                                 final M checkpointMark) {
    this.unboundedSource = unboundedSource;
    this.pipelineOptions = options;
    this.checkpointMark = checkpointMark;
  }

  public UnboundedSource.UnboundedReader<O> getReader() {
    return reader;
  }

  public UnboundedSource getUnboundedSource() {
    return unboundedSource;
  }

  @Override
  public void prepare() {
    LOG.info("Prepare unbounded sources!! {}, {}", unboundedSource, unboundedSource.toString());
    try {
      readableService = ReadableService.getInstance();
      reader = unboundedSource.createReader(pipelineOptions, checkpointMark);
      kafkaReader = (KafkaUnboundedReader) reader;

      isCurrentAvailable = reader.start();

    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isAvailable() {

    LOG.info("unboudned source available: {}, {}", reader, isCurrentAvailable);
    if (reader == null) {
      return false;
    }

    if (isCurrentAvailable) {
      return true;
    } else {
      try {
        isCurrentAvailable =  reader.advance();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      return isCurrentAvailable;
    }
  }


  @Override
  public Object readCurrent() {

    if (isCurrentAvailable) {
      final O elem = reader.getCurrent();
      final Instant currTs = reader.getCurrentTimestamp();
      //LOG.info("Curr timestamp: {}", currTs);

      try {
        isCurrentAvailable =  reader.advance();
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      return new TimestampAndValue<>(currTs.getMillis(),
        WindowedValue.timestampedValueInGlobalWindow(elem, reader.getCurrentTimestamp()));
    } else {
      if (!isKafkaPolling) {
        isKafkaPolling = true;
        readableService.execute(() -> {

          // poll kafka
          kafkaReader.pollRecord(5);

          // set current available
          try{
            isCurrentAvailable = reader.advance();
          } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }

          isKafkaPolling = false;
        });
      }

      return EmptyElement.getInstance();
    }

    /*
    if (isFetchTime) {
      isFetchTime = false;
      readableService.execute(() -> {
        try {
          isCurrentAvailable = reader.advance();
          isFetchTime = !isCurrentAvailable;
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      });
    }

    if (isCurrentAvailable) {
      final O elem = reader.getCurrent();
      final Instant currTs = reader.getCurrentTimestamp();
      //LOG.info("Curr timestamp: {}", currTs);

      isCurrentAvailable = false;
      readableService.execute(() -> {
        try {
          isCurrentAvailable = reader.advance();
          isFetchTime = !isCurrentAvailable;
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      });

      return new TimestampAndValue<>(currTs.getMillis(),
        WindowedValue.timestampedValueInGlobalWindow(elem, reader.getCurrentTimestamp()));
    }

    return EmptyElement.getInstance();
    */
  }

  @Override
  public long readWatermark() {
    final Instant watermark = reader.getWatermark();
    // Finish if the watermark == TIMESTAMP_MAX_VALUE
    isFinished = (watermark.getMillis() >= GlobalWindow.TIMESTAMP_MAX_VALUE.getMillis());
    return watermark.getMillis();
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }

  @Override
  public List<String> getLocations() throws Exception {
    return new ArrayList<>();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
