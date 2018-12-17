package com.spark.kinesis.learn;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;

public class KinesisProducer {

	public static void writeToLocalStream(String streamName,String kinesisEndPointUrl ,String message)
	{
		
		AmazonKinesis kinesisClient = new AmazonKinesisClient(new BasicAWSCredentials("TestAccessKey","TestSecretKey"));
		kinesisClient.setEndpoint(kinesisEndPointUrl);
		System.out.println(kinesisClient.listStreams());
		byte[] bytes = message.getBytes();
		PutRecordRequest putRecord = new PutRecordRequest();
		putRecord.setStreamName(streamName);
		putRecord.setPartitionKey("needToBeUpdated");
		putRecord.setData(ByteBuffer.wrap(bytes));

		try {
			PutRecordResult putRecordsResult = kinesisClient.putRecord(putRecord);
			System.out.println("Put Result: " + putRecordsResult);
		} catch (AmazonClientException ex) {
			System.out.println("Error sending record to Amazon Kinesis. " + ex);
		}
	}
	
	public static void main(String[] args) throws IOException {
		String kinesisEndPointUrl="https://kinesis.ap-southeast-2.amazonaws.com";   //Change the port of your local running kinesis stream
		String streamName="testStream"; //Name of the stream from which you want to read records
		BufferedReader br=new BufferedReader(new FileReader("D:\\Projects\\sample_records.txt"));
		String line="";
		while((line=br.readLine())!=null)
		{
		writeToLocalStream(streamName,kinesisEndPointUrl,line);
		}
		br.close();
	}
}
