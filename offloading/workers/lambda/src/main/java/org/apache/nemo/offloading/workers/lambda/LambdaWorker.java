package org.apache.nemo.offloading.workers.lambda;

//import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3ClientBuilder;
//import com.amazonaws.services.s3.model.S3Object;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.nemo.offloading.common.OffloadingHandler;

import java.util.HashMap;
import java.util.Map;

public class LambdaWorker implements RequestHandler<Map<String, Object>, Object> {

	private static final Logger LOG = LogManager.getLogger(LambdaWorker.class);
	//private static final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
  //  .withClientConfiguration(new ClientConfiguration().withMaxConnections(10)).build();

	private static final String BUCKET_NAME = "nemo-serverless";
	private static final String PATH = "/tmp/nexmark-0.2-SNAPSHOT-shaded.jar";
	//private static final String PATH = "/tmp/shaded.jar";

  private final OffloadingHandler offloadingHandler;

	private ClassLoader createClassLoader() {
	  /*
		// read jar file
		//final S3Object result = s3Client.getObject(BUCKET_NAME, "jars/shaded.jar");
		if (!Files.exists(Paths.get(PATH))) {
			LOG.info("Copying file...");
      final S3Object result = s3Client.getObject(BUCKET_NAME, "jars/nexmark-0.2-SNAPSHOT-shaded.jar");
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
			return new URLClassLoader(urls, this.getClass().getClassLoader());
		} catch (MalformedURLException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		*/
	  return null;
  }

	public LambdaWorker() {
    this.offloadingHandler = new OffloadingHandler(new HashMap<>());
	}


	@Override
	public Object handleRequest(Map<String, Object> input, Context context) {
	  return offloadingHandler.handleRequest(input);
	}
}
