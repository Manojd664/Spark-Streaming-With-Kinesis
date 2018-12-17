package com.spark.kinesis.learn;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory;
import com.amazonaws.services.kinesis.model.Record;

public class KinesisKCLConsumer implements IRecordProcessorFactory {
	private static final Logger LOGGER = Logger.getLogger(KinesisKCLConsumer.class.getName());
	private Worker.Builder builder;

	

	static class CredProvider implements AWSCredentialsProvider {
		AWSCredentials _creds;

		public CredProvider(AWSCredentials creds) {
			_creds = creds;
		}

		public AWSCredentials getCredentials() {
			return _creds;
		}

		public void refresh() {
			// NOOP
		}
	}

	public KinesisKCLConsumer(String dynamoDbEndPointUrl, String kinesisEndPointUrl, String streamName,
			String appName) {
		AWSCredentials credentials = new BasicAWSCredentials("TestAccessKey", "secretAccessKey");
		CredProvider credentialsProvider = new CredProvider(credentials);
		AmazonDynamoDBClient dbLocalClient = new AmazonDynamoDBClient(credentials);
		dbLocalClient.setEndpoint(dynamoDbEndPointUrl);

	    AmazonKinesis kinesisClient = new AmazonKinesisClient();
	    kinesisClient.setEndpoint(kinesisEndPointUrl);

	    KinesisClientLibConfiguration clientConfig =
	            new KinesisClientLibConfiguration(appName,streamName, credentialsProvider, "my-worker")
	                    .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

	    this.builder = new Worker.Builder()
	            .recordProcessorFactory(this)
	            .config(clientConfig)
	            .dynamoDBClient(dbLocalClient)
	            .kinesisClient(kinesisClient)
	            .metricsFactory(new NullMetricsFactory());
	}

	public void start() {
		this.builder.build().run();

	}

	public IRecordProcessor createProcessor() {
		return new IRecordProcessor() {

			public void initialize(String shardId) {
			}

			public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
				System.out.println("Records: ");
				for (Record record : records) {
					byte[] bytes = new byte[record.getData().remaining()];
					record.getData().get(bytes);
					String data = new String(bytes);
					System.out.println(data);
				}
				try {
					
					checkpointer.checkpoint();
				} catch (Exception e1) {
					e1.printStackTrace();
				}

			}

			public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
				LOGGER.log(Level.SEVERE, "Shutting down");
			}

		};

	}
	
	public static void main(String[] args) {
		String dynamoDbEndPointUrl="https://kinesis.ap-southeast-2.amazonaws.com"; //Change the port of your local running dynamodb
		String kinesisEndPointUrl="https://kinesis.ap-southeast-2.amazonaws.com";   //Change the port of your local running kinesis stream
		String streamName="testStream"; //Name of the stream from which you want to read records
		String applicationName="testApp"; //Dynamo DB table will be created with this name to store checkpoint
		KinesisKCLConsumer kc=new KinesisKCLConsumer(dynamoDbEndPointUrl,kinesisEndPointUrl,streamName,applicationName);
		
		System.out.println("***Starting reading***");
		kc.start();
	}
}
