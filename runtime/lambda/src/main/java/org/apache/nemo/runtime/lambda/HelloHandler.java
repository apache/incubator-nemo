package org.apache.nemo.runtime.lambda;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.nemo.common.coder.DecoderFactory;
import org.apache.nemo.common.ir.OutputCollector;
import org.apache.nemo.common.lambda.LambdaDecoderFactory;
import org.apache.nemo.common.lambda.SerializeUtils;
import org.apache.nemo.common.punctuation.Watermark;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HelloHandler implements RequestHandler<Map<String, Object>, Object> {

	private static final Logger LOG = LogManager.getLogger(HelloHandler.class);
	//private static final OutputSender sender = new OutputSender("18.182.129.182", 20312);
	private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
    .withClientConfiguration(new ClientConfiguration().withMaxConnections(100)).build();

	private static final String BUCKET_NAME = "nemo-serverless";
	private static final String PATH = "/tmp/nexmark-0.2-SNAPSHOT-shaded.jar";
	//private static final String PATH = "/tmp/shaded.jar";
	private URLClassLoader classLoader = null;
	private LambdaSideInputHandler handler = null;

	private final String serializedUserCode = "rO0ABXNyABZRdWVyeTdTaWRlSW5wdXRIYW5kbGVyMlM6Ib0vAkQCAAB4cA==";

	private void createClassLoader() {
		// read jar file
		final S3Object result = s3Client.getObject(BUCKET_NAME, "jars/nexmark-0.2-SNAPSHOT-shaded.jar");
		//final S3Object result = s3Client.getObject(BUCKET_NAME, "jars/shaded.jar");
		if (!Files.exists(Paths.get(PATH))) {
			LOG.info("Copying file...");
			final InputStream in = result.getObjectContent();
			try {
				Files.copy(in, Paths.get(PATH));
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}

		try {
			final URL[] urls = new URL[1];
			final File f = new File(PATH);
      urls[0] = f.toURI().toURL();
			LOG.info("File: {}, {}", f.toPath(), urls[0]);
			classLoader = new URLClassLoader(urls, this.getClass().getClassLoader());
		} catch (MalformedURLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
  }



	public HelloHandler() {
		LOG.info("Handler is created! {}", this);
	}

	@Override
	public Object handleRequest(Map<String, Object> input, Context context) {
		System.out.println("Input: " + input);

		if (classLoader == null) {
			createClassLoader();
			handler = SerializeUtils.deserializeFromString(serializedUserCode, classLoader);
			LOG.info("Create class loader: {}", classLoader);
		}


		if (input.isEmpty()) {
		  // this is warmer, just return;
      System.out.println("Warm up");
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    }

		final String sideInputKey = (String) input.get("sideInput");
		final String mainInputKey = (String) input.get("mainInput");
		final List<String> result = new ArrayList<>();
    final OutputCollector outputCollector = new LambdaOutputHandler(result);

		try {
			final S3Object sideInputS3 = s3Client.getObject(BUCKET_NAME, sideInputKey);
      final S3Object mainInputS3 = s3Client.getObject(BUCKET_NAME, mainInputKey);
			final S3ObjectInputStream sideInputStream = sideInputS3.getObjectContent();
      final S3ObjectInputStream mainInputStream = mainInputS3.getObjectContent();

			// get decoder factory
      final LambdaDecoderFactory sideInputDecoderFactory =
        SerializeUtils.deserialize(sideInputStream, classLoader);

      final LambdaDecoderFactory mainInputDecoderFactory =
        SerializeUtils.deserialize(mainInputStream, classLoader);

		  final DecoderFactory.Decoder sideInputDecoder =
        sideInputDecoderFactory.create(sideInputStream);

		  final DecoderFactory.Decoder mainInputDecoder =
        mainInputDecoderFactory.create(mainInputStream);

		  final WindowedValue sideInput = (WindowedValue) sideInputDecoder.decode();

		  //System.out.println("Side input: " + sideInput);

      while (true) {
        try {
          final WindowedValue mainInput = (WindowedValue) mainInputDecoder.decode();
          handler.processMainAndSideInput(mainInput, sideInput, outputCollector);
          //System.out.println("Windowed value: " + mainInput);
        } catch (final IOException e) {
          if(e.getMessage().contains("EOF")) {
            System.out.println("eof!");
          } else {
            throw e;
          }
          break;
        }
      }

      s3Client.deleteObject(BUCKET_NAME, mainInputKey);
      System.out.println("Delete key " + mainInputKey);


		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

		//final Decoder object  = (T)ois.readObject();
		//ois.close();
		//return object;

    return result.toString();

	}


	final class LambdaOutputHandler  implements OutputCollector {

	  private final List<String> result;

	  public LambdaOutputHandler(final List<String> result) {
	    this.result = result;
    }

    @Override
    public void emit(Object output) {
      System.out.println("Emit output: " + output);
      result.add(output.toString());
    }

    @Override
    public void emitWatermark(Watermark watermark) {

    }

    @Override
    public void emit(String dstVertexId, Object output) {

    }
  }
}
