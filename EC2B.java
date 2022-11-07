package com.aws.imageRecognition;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.amazon.sqs.javamessaging.*;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.*;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import java.util.*;
import com.amazonaws.services.rekognition.model.*;
import javax.jms.*;
import javax.jms.Queue;

import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;

@SpringBootApplication(exclude = {
		org.springframework.cloud.aws.autoconfigure.context.ContextInstanceDataAutoConfiguration.class,
		org.springframework.cloud.aws.autoconfigure.context.ContextStackAutoConfiguration.class,
		org.springframework.cloud.aws.autoconfigure.context.ContextRegionProviderAutoConfiguration.class })

public class AWSTextRekognition {
	public static void main(String[] args) throws Exception {
		SpringApplication.run(AWSTextRekognition.class, args);

		Regions clientRegion = Regions.US_EAST_1;
		String queueName = "SgQueue.fifo";
		try {

			AmazonSQS sqsClient = AmazonSQSClientBuilder.standard().withCredentials(new ProfileCredentialsProvider())
					.withRegion(clientRegion).build();
			try {

				SQSConnectionFactory connectionFactory = new SQSConnectionFactory(new ProviderConfiguration(),
						AmazonSQSClientBuilder.defaultClient());
				SQSConnection connection = connectionFactory.createConnection();
				AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();

				if (!client.queueExists(queueName)) {
					Map<String, String> attributes = new HashMap<String, String>();
					attributes.put("FifoQueue", "true");
					attributes.put("ContentBasedDeduplication", "true");
					client.createQueue(new CreateQueueRequest().withQueueName(queueName).withAttributes(attributes));
				}

				Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

				// Create a queue identity and specify the queue name to the session
				Queue queue = session.createQueue(queueName);
				MessageConsumer consumer = session.createConsumer(queue);
				consumer.setMessageListener(new MyListener());

				// receiving incoming messages.
				connection.start();

				Thread.sleep(10000);

			} catch (Exception e) {
				System.out.println(
						"Please run the instance 1, Instance 2 will be waiting for Instance 1 for loading images in SQS");
				SQSConnectionFactory connectionFactory = new SQSConnectionFactory(new ProviderConfiguration(),
						AmazonSQSClientBuilder.defaultClient());
				SQSConnection connection = connectionFactory.createConnection();
				AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();
				if (!client.queueExists(queueName)) {
					Map<String, String> attributes = new HashMap<String, String>();
					attributes.put("FifoQueue", "true");
					attributes.put("ContentBasedDeduplication", "true");
					client.createQueue(
							new CreateQueueRequest().withQueueName("MyQueue.fifo").withAttributes(attributes));
					Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
					// Create a queue identity and specify the queue name to the session
					Queue queue = session.createQueue(queueName);

					// Create a consumer for the 'MyQueue'.
					MessageConsumer consumer = session.createConsumer(queue);

					// Instantiate and set the message listener for the consumer.
					consumer.setMessageListener(new MyListener());

					// Start receiving incoming messages.
					connection.start();

					Thread.sleep(10000);
				}
			}
		} catch (AmazonServiceException e) {
			System.out.println("Please run the instance 1 first");
		}
	}
}

class MyListener implements MessageListener {

	public void onMessage(Message message) {
		try {
			Regions clientRegion = Regions.US_EAST_1;
			String bucketName = "njit-cs-643";

			ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
			ListObjectsV2Result result;

			AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(clientRegion)
					.withCredentials(new ProfileCredentialsProvider()).build();

			AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();

			result = s3Client.listObjectsV2(req);
			for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
				String m = (String) ((TextMessage) message).getText().toString();
				if (objectSummary.getKey().contains(m)) {
					String photo = objectSummary.getKey();

					DetectTextRequest request = new DetectTextRequest()
							.withImage(new Image().withS3Object(new S3Object().withName(photo).withBucket(bucketName)));
					try {
						DetectTextResult result1 = rekognitionClient.detectText(request);
						List<TextDetection> textDetections = result1.getTextDetections();
						if (!textDetections.isEmpty()) {
							System.out.print("Image with Text:  " + photo + " ==> ");
							for (TextDetection text : textDetections) {
								System.out.println("  Found Text : " + text.getDetectedText() + " , Confidence Level: "
										+ text.getConfidence().toString());
								System.out.println();
							}
						}
					} catch (AmazonRekognitionException e) {
						System.out.print("Problem in Image Detection using Rekognition");
						e.printStackTrace();
					}
				}
			}

		} catch (JMSException e) {
			System.out.println("JMS Exception: Please start Instance 1 for loading the messages in the queue");
		}
	}
}