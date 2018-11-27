package org.apache.nemo.runtime.executor.datatransfer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.nemo.common.coder.EncoderFactory;
import org.apache.nemo.runtime.executor.data.SerializerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

public final class S3StorageObjectFactory implements StorageObjectFactory {
  private static final Logger LOG = LoggerFactory.getLogger(S3StorageObjectFactory.class.getName());
  private static final String S3_BUCKET_NAME = "nemo-serverless";
  private final AmazonS3 amazonS3;

  public static final S3StorageObjectFactory INSTACE = new S3StorageObjectFactory();

  private S3StorageObjectFactory() {
    this.amazonS3 = AmazonS3ClientBuilder.standard()
      .withClientConfiguration(new ClientConfiguration().withMaxConnections(150)).build();
  }

  @Override
  public StorageObject newInstance(String prefix,
                                   String suffix,
                                   int partition,
                                   byte[] encodedDecoderFactory,
                                   EncoderFactory encoderFactory) {
    return new S3StorageObject(prefix + suffix,
      partition, encodedDecoderFactory, encoderFactory);
  }

  @Override
  public SideInputProcessor sideInputProcessor(SerializerManager serializerManager, String edgeId) {
    return new S3SideInputProcessor(serializerManager, edgeId);
  }

  final class S3StorageObject implements StorageObjectFactory.StorageObject {
    private final EncoderFactory.Encoder encoder;
    //private final OutputStream outputStream;
    private final ByteArrayOutputStream bos;

    public final String fname;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final int partition;
    private final String prefix;

    public S3StorageObject(final String prefix, final int partition,
                           final byte[] encodedDecoderFactory,
                           final EncoderFactory encoderFactory) {
      this.prefix = prefix;
      this.fname = prefix + "-" + partition;
      this.partition = partition;
      try {
        this.bos = new ByteArrayOutputStream();
        bos.write(encodedDecoderFactory);
        this.encoder = encoderFactory.create(bos);
        /*
        this.outputStream = new FileOutputStream(fname);
        outputStream.write(encodedDecoderFactory);
        this.encoder = encoderFactory.create(outputStream);
        */
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void encode(Object object) {
      try {
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
          //dbos.close();
          //LOG.info("Output Stream bytes: {}", dbos.getCount());
          //dbos.writeTo(outputStream);

          bos.close();
          final InputStream is = new ByteArrayInputStream(bos.toByteArray());
          final ObjectMetadata metadata = new ObjectMetadata();
          metadata.setContentLength(bos.size());
          final PutObjectRequest putObjectRequest =
            new PutObjectRequest(S3_BUCKET_NAME + "/maininput", fname, is, metadata);
          amazonS3.putObject(putObjectRequest);
          LOG.info("End of send main input to S3 {}/{}", fname, bos.size());
          is.close();

          /*
          outputStream.close();
          final File file = new File(fname);
          LOG.info("Start to send main input data to S3 {}", file.getName());
          final PutObjectRequest putObjectRequest =
            new PutObjectRequest(S3_BUCKET_NAME + "/maininput", file.getName(), file);
          amazonS3.putObject(putObjectRequest);
          file.delete();
          LOG.info("End of send main input to S3 {}", file.getName());
          */

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

}



