package org.apache.nemo.runtime.lambda;

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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Map;

public class HelloHandler implements RequestHandler<Map<String, Object>, Object> {

	private static final Logger LOG = LogManager.getLogger(HelloHandler.class);
	//private static final OutputSender sender = new OutputSender("18.182.129.182", 20312);
	private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

	private static final String BUCKET_NAME = "nemo-serverless";
	private static final String PATH = "/tmp/nexmark-0.1-SNAPSHOT-shaded.jar";
	//private static final String PATH = "/tmp/shaded.jar";
	private URLClassLoader classLoader = null;


	private void createClassLoader() {
		// read jar file
		final S3Object result = s3Client.getObject(BUCKET_NAME, "jars/nexmark-0.1-SNAPSHOT-shaded.jar");
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
		LOG.info("Input: {}", input);

		System.out.println("Input: " + input);

		if (classLoader == null) {
			createClassLoader();
			LOG.info("Create class loader: {}", classLoader);
		}

		final String s3Key = (String) input.get("input");
		final LambdaDecoderFactory lambdaDecoderFactory =
      SerializeUtils.deserializeFromString((String) input.get("decoder"), classLoader);

		System.out.println("decoder factory: " + lambdaDecoderFactory);

		try {
			final S3Object result = s3Client.getObject(BUCKET_NAME, s3Key);
			final S3ObjectInputStream inputStream = result.getObjectContent();
		  final DecoderFactory.Decoder decoder = lambdaDecoderFactory.create(inputStream);
      System.out.println("Decoder " + decoder);

      while (true) {
        final WindowedValue value = (WindowedValue) decoder.decode();

        if (value == null) {
          break;
        }

        System.out.println("Windowed value: " + value);
      }

			/*
			final Method createMethod = object.getClass().getMethod("create", InputStream.class);
			final Object decoder = createMethod.invoke(object, inputStream);

			LOG.info("Decoder {}", decoder);

			final Method decodeMethod = decoder.getClass().getMethod("decode");
			final Object decodedValue = decodeMethod.invoke(decoder);
			LOG.info("Decode value!: {}", decodedValue);
			*/

		} catch (IOException e) {
			e.printStackTrace();
		}
		//final Decoder object  = (T)ois.readObject();
		//ois.close();
		//return object;


		return null;
	}
}
