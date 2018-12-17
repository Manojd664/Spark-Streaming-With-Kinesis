package com.spark.kinesis.learn;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kinesis.KinesisUtils;

public class KinesisSparkConsumer {
	/**
	 * Checks if the stream exists and is active
	 *
	 * @param kinesisClient
	 *            Amazon Kinesis client instance
	 * @param streamName
	 *            Name of stream
	 */
	private static void validateStream(AmazonKinesis kinesisClient, String streamName) {
		try {
			DescribeStreamResult result = kinesisClient.describeStream(streamName);
			if (!"ACTIVE".equals(result.getStreamDescription().getStreamStatus())) {
				System.err.println("Stream " + streamName + " is not active. Please wait a few moments and try again.");
				System.exit(1);
			}
		} catch (ResourceNotFoundException e) {
			System.err.println("Stream " + streamName + " does not exist. Please create it in the console.");
			System.err.println(e);
			System.exit(1);
		} catch (Exception e) {
			System.err.println("Error found while describing the stream " + streamName);
			System.err.println(e);
			System.exit(1);
		}
	}
	public static void readFromLocalStream(String kinesisEndPointUrl,String streamName,String kinesisAppName)
	{
		String regionName = "ap-southeast-2";

	    AmazonKinesis kinesisClient = new AmazonKinesisClient(new BasicAWSCredentials("TestAccessKey", "TestSecretKey"));
	    kinesisClient.setEndpoint(kinesisEndPointUrl);

	    validateStream(kinesisClient, streamName);
	    int numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size();

		int numStreams = numShards;

		Duration batchInterval = new Duration(10000);

		Duration kinesisCheckpointInterval = batchInterval;
		
		SparkConf sparkConfig = new SparkConf().setAppName("JavaKinesisReader").setMaster("local[4]");
		
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConfig, batchInterval);

	    List<JavaDStream<byte[]>> streamsList = new ArrayList<>(numStreams);
	    for (int i = 0; i < numStreams; i++) {
	      streamsList.add(
	    		  KinesisUtils.createStream(jssc, kinesisAppName, streamName, kinesisEndPointUrl, regionName,
	  					InitialPositionInStream.TRIM_HORIZON, kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2())
	      );
	    }
	    JavaDStream<byte[]> kinesisStream;
	    if (streamsList.size() > 1) {
	    	kinesisStream = jssc.union(streamsList.get(0), streamsList.subList(1, streamsList.size()));
	    } else {
	    	kinesisStream = streamsList.get(0);
	    }
		kinesisStream.foreachRDD( new VoidFunction<JavaRDD<byte[]>>() {
			@Override
			public void call(JavaRDD<byte[]> rdd) throws Exception {
				System.out.println("Displaying Records");
				rdd.collect().forEach(bytes->System.out.println(new String(bytes,StandardCharsets.UTF_8)));
			}});
		
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args)
	{
		String kinesisEndPointUrl="https://kinesis.ap-southeast-2.amazonaws.com";  //Change the port of your local running kinesis stream
		String streamName="testStream";   //Name of the stream from which you want to read records
		String applicationName="testApp"; //Dynamo DB table will be created with this name to store checkpoint
		readFromLocalStream(kinesisEndPointUrl,streamName,applicationName);
	}
}
