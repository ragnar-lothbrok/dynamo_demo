package com.aws.dynamodb.Impl;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.aws.dynamodb.DynamoDBClient;
import com.aws.dynamodb.api.DynamoApi;
import com.aws.dynamodb.dto.TableRequestDto;
import com.aws.dynamodb.dto.TableRequestDto.AttributeDetail;
import com.aws.dynamodb.enums.QueryTypeEnum;

public class DynamoServiceImpl implements DynamoApi {

	public Boolean createTable(TableRequestDto tableDto) {
		AmazonDynamoDB dynamoDB = DynamoDBClient.getDynamoDBClientInstance();

		CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableDto.getTableName());
		for (AttributeDetail attributeDetail : tableDto.getAttributes()) {
			if (KeyType.HASH.equals(attributeDetail.getKeyType())
					|| KeyType.RANGE.equals(attributeDetail.getKeyType())) {
				createTableRequest
						.withKeySchema(new KeySchemaElement().withAttributeName(attributeDetail.getAttributeName())
								.withKeyType(attributeDetail.getKeyType()));
				createTableRequest.withAttributeDefinitions(
						new AttributeDefinition().withAttributeName(attributeDetail.getAttributeName())
								.withAttributeType(attributeDetail.getScalarAttributeType()));
			}
		}

		createTableRequest.withProvisionedThroughput(new ProvisionedThroughput()
				.withReadCapacityUnits(tableDto.getReadCapacity()).withWriteCapacityUnits(tableDto.getWriteCapacity()));

		// Create table if it does not exist yet
		Boolean tableCreated = TableUtils.createTableIfNotExists(dynamoDB, createTableRequest);
		System.out.println("Table created : " + tableCreated);

		// wait for the table to move into ACTIVE state
		try {
			TableUtils.waitUntilActive(dynamoDB, tableDto.getTableName());
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to AWS, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (InterruptedException e) {
			System.out.println("Exception occured while trying to create table in dynamo database = " + e);
		}
		return tableCreated;
	}

	public TableDescription getTableDescription(DescribeTableRequest describeTableRequest) {
		AmazonDynamoDB dynamoDB = DynamoDBClient.getDynamoDBClientInstance();
		TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
		System.out.println("Table Description: " + tableDescription);
		return tableDescription;
	}

	
	public <T> PutItemResult createItem(String tableName, T t)
			throws IllegalArgumentException, IllegalAccessException, SecurityException {
		Map<String, AttributeValue> item = null;
		if (t != null) {
			Class<? extends Object> klass = t.getClass();
			item = new HashMap<String, AttributeValue>();
			for (Field field : klass.getDeclaredFields()) {
				field.setAccessible(true);
				if (field.getName().indexOf("serialVersionUID") == -1)
					setValues(field, t, item);
			}
		}
		PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
		return DynamoDBClient.getDynamoDBClientInstance().putItem(putItemRequest);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <T> void setValues(Field field, T t, Map<String, AttributeValue> item)
			throws IllegalArgumentException, IllegalAccessException {
		if (field.getType() == Boolean.class || field.getType() == Float.class || field.getType() == Double.class
				|| field.getType() == Integer.class) {
			item.put(field.getName(), new AttributeValue().withN(field.get(t).toString()));
		} else if (field.getType() == String.class) {
			item.put(field.getName(), new AttributeValue(((String) field.get(t))));
		} else if (List.class == field.getType()) {
			item.put(field.getName(), new AttributeValue().withSS((ArrayList) field.get(t)));
		}
	}

	@SuppressWarnings("rawtypes")
	
	public AmazonWebServiceResult getData(String tableName, String name, String value, QueryTypeEnum queryTypeEnum) {
		AmazonDynamoDB dynamoDB = DynamoDBClient.getDynamoDBClientInstance();
		HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
		if (name != null) {
			Condition condition = new Condition().withComparisonOperator(ComparisonOperator.EQ.toString())
					.withAttributeValueList(new AttributeValue().withS(value));
			scanFilter.put(name, condition);
		}
		if (queryTypeEnum == null || QueryTypeEnum.QUERY.equals(queryTypeEnum)) {
			QueryRequest queryRequest = new QueryRequest().withTableName(tableName).withConsistentRead(true);
			if (scanFilter.size() > 0) {
				queryRequest.withKeyConditions(scanFilter);
			}
			QueryResult queryResult = dynamoDB.query(queryRequest);
			System.out.println("Result: " + queryResult);
			return ((AmazonWebServiceResult) queryResult);
		} else {
			ScanRequest scanRequest = new ScanRequest(tableName).withConsistentRead(true);
			if (scanFilter.size() > 0) {
				scanRequest = scanRequest.withScanFilter(scanFilter);
			}
			ScanResult scanResult = dynamoDB.scan(scanRequest);
			System.out.println("Result: " + scanResult);
			return ((AmazonWebServiceResult) scanResult);
		}
	}

	public <T> UpdateItemResult updateItem(String tableName, List<String> keyNames, T t)
			throws IllegalArgumentException, IllegalAccessException, SecurityException {
		Map<String, AttributeValueUpdate> updates = null;
		Map<String, AttributeValue> key = null;
		if (t != null) {
			Class<? extends Object> klass = t.getClass();
			key = new HashMap<String, AttributeValue>();
			updates = new HashMap<String, AttributeValueUpdate>();
			for (Field field : klass.getDeclaredFields()) {
				field.setAccessible(true);
				if (keyNames.contains(field.getName())) {
					setValues(field, t, key);
				} else if (field.getName().indexOf("serialVersionUID") == -1)
					setUpdatedValues(field, t, updates);
			}
		}
		UpdateItemRequest updateItemRequest = new UpdateItemRequest(tableName, key, updates);
		return DynamoDBClient.getDynamoDBClientInstance().updateItem(updateItemRequest);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <T> void setUpdatedValues(Field field, T t, Map<String, AttributeValueUpdate> item)
			throws IllegalArgumentException, IllegalAccessException {
		if (field.getType() == Boolean.class || field.getType() == Float.class || field.getType() == Double.class
				|| field.getType() == Integer.class) {
			item.put(field.getName(),
					new AttributeValueUpdate(new AttributeValue().withN(field.get(t).toString()), AttributeAction.PUT));
		} else if (field.getType() == String.class) {
			item.put(field.getName(),
					new AttributeValueUpdate(new AttributeValue(((String) field.get(t))), AttributeAction.PUT));
		} else if (List.class == field.getType()) {
			item.put(field.getName(), new AttributeValueUpdate(new AttributeValue().withSS((ArrayList) field.get(t)),
					AttributeAction.PUT));
		}
	}

	
	public ListTablesResult listDynamoTables(Integer limit) {
		ListTablesRequest listTablesRequest = new ListTablesRequest().withLimit(limit);
		return DynamoDBClient.getDynamoDBClientInstance().listTables(listTablesRequest);
	}

	
	public DeleteTableResult deleteTable(String tableName) {
		DeleteTableRequest deleteTableRequest = new DeleteTableRequest();
		deleteTableRequest.setTableName(tableName);
		return DynamoDBClient.getDynamoDBClientInstance().deleteTable(deleteTableRequest);
	}

	
	public <T> DeleteItemResult deleteItem(String tableName, List<String> keyNames, T t)
			throws IllegalArgumentException, IllegalAccessException {
		DeleteItemRequest deleteItemRequest = new DeleteItemRequest();
		deleteItemRequest.setTableName(tableName);
		Map<String, AttributeValue> item = null;
		if (t != null) {
			Class<? extends Object> klass = t.getClass();
			item = new HashMap<String, AttributeValue>();
			for (Field field : klass.getDeclaredFields()) {
				field.setAccessible(true);
				if (keyNames.contains(field.getName())) {
					setValues(field, t, item);
					break;
				}
			}
		}
		item.put("price", new AttributeValue().withN("12000"));
		deleteItemRequest.withKey(item);
		return DynamoDBClient.getDynamoDBClientInstance().deleteItem(deleteItemRequest);
	}

	
	public <T> BatchWriteItemResult createItemInBatch(String tableName, List<T> ts)
			throws IllegalArgumentException, IllegalAccessException {
		Map<String, List<WriteRequest>> batchedItems = new HashMap<String, List<WriteRequest>>();
		List<WriteRequest> writeRequests = new ArrayList<WriteRequest>();
		for (T t : ts) {
			Map<String, AttributeValue> item = null;
			if (t != null) {
				Class<? extends Object> klass = t.getClass();
				item = new HashMap<String, AttributeValue>();
				for (Field field : klass.getDeclaredFields()) {
					field.setAccessible(true);
					if (field.getName().indexOf("serialVersionUID") == -1)
						setValues(field, t, item);
				}
			}
			PutRequest putRequest = new PutRequest().withItem(item);
			writeRequests.add(new WriteRequest().withPutRequest(putRequest));
		}
		batchedItems.put(tableName, writeRequests);

		return DynamoDBClient.getDynamoDBClientInstance().batchWriteItem(batchedItems);
	}

}