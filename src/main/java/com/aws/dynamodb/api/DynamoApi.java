package com.aws.dynamodb.api;

import java.util.List;

import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.aws.dynamodb.dto.TableRequestDto;
import com.aws.dynamodb.enums.QueryTypeEnum;

public interface DynamoApi {

	Boolean createTable(TableRequestDto tableDto);
	
	<T> BatchWriteItemResult createItemInBatch(String tableName, List<T> t) throws IllegalArgumentException, IllegalAccessException;
	

	TableDescription getTableDescription(DescribeTableRequest describeTableRequest);

	<T> PutItemResult createItem(String tableName, T t)
			throws IllegalArgumentException, IllegalAccessException, SecurityException;

	AmazonWebServiceResult getData(String tableName, String name, String value, QueryTypeEnum queryTypeEnum);


	<T> UpdateItemResult updateItem(String tableName, List<String> keyNames, T t)
			throws IllegalArgumentException, IllegalAccessException, SecurityException;
	
	ListTablesResult listDynamoTables(Integer limit);
	
	DeleteTableResult deleteTable(String tableName);
	

	<T> DeleteItemResult deleteItem(String tableName, List<String> keyNames, T t) throws IllegalArgumentException, IllegalAccessException;
	
}
