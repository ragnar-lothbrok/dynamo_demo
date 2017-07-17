package com.aws.dynamodb;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

public class DynamoDBClient {

	private static volatile AmazonDynamoDB amazonDynamoDB = null;

	private DynamoDBClient() {

	}

	public static AmazonDynamoDB getDynamoDBClientInstance() {
		if (amazonDynamoDB == null) {
			synchronized (DynamoDBClient.class) {
				if (amazonDynamoDB == null) {
					amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
							.withEndpointConfiguration(
									new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
							.build();
				}
			}
		}
		return amazonDynamoDB;
	}
}
