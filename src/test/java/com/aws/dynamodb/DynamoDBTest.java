package com.aws.dynamodb;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.aws.dynamodb.Impl.DynamoServiceImpl;
import com.aws.dynamodb.dto.Product;
import com.aws.dynamodb.dto.TableRequestDto;
import com.aws.dynamodb.dto.TableRequestDto.AttributeDetail;
import com.aws.dynamodb.enums.QueryTypeEnum;

public class DynamoDBTest {

	@Test
	public void testCreate() {
		TableRequestDto tableRequestDto = new TableRequestDto("product", 4l, 4l);
		tableRequestDto.getAttributes().add(new AttributeDetail("productId", KeyType.HASH, ScalarAttributeType.S));
		tableRequestDto.getAttributes().add(new AttributeDetail("price", KeyType.RANGE, ScalarAttributeType.N));
		DynamoServiceImpl dynamoServiceImpl = new DynamoServiceImpl();
		assertEquals(false, dynamoServiceImpl.createTable(tableRequestDto));
	}

	@Test
	public void testCreateItem() throws IllegalArgumentException, IllegalAccessException, SecurityException {
		Product product = new Product();
		product.setColor("Blue");
		product.setProductId("ABCDE124113");
		product.setDescription("Jeans good quality");
		product.setMrp(1299f);
		product.setPrice(12000f);
		product.getSize().add("X");
		product.getSize().add("XL");
		product.getSize().add("XXL");
		product.getSize().add("M");
		product.getSize().add("S");
		DynamoServiceImpl dynamoServiceImpl = new DynamoServiceImpl();
		PutItemResult putItemBlog = dynamoServiceImpl.createItem("product", product);
		assertEquals(200, putItemBlog.getSdkHttpMetadata().getHttpStatusCode());
	}

	@Test
	public void testBatchCreateItem() throws IllegalArgumentException, IllegalAccessException, SecurityException {
		List<Product> products = new ArrayList<Product>();
		for (int i = 0; i < 3; i++) {
			Product product = new Product();
			product.setColor("Blue");
			product.setProductId("ABCDE" + new Random().nextInt(1000));
			product.setDescription("Jeans good quality");
			product.setMrp(1299f);
			product.setPrice(12000f);
			product.getSize().add("X");
			product.getSize().add("XL");
			product.getSize().add("XXL");
			product.getSize().add("M");
			product.getSize().add("S");
			products.add(product);
		}
		DynamoServiceImpl dynamoServiceImpl = new DynamoServiceImpl();
		BatchWriteItemResult batchWriteItemResult = dynamoServiceImpl.createItemInBatch("product", products);
		assertEquals(200, batchWriteItemResult.getSdkHttpMetadata().getHttpStatusCode());
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testGetItem() {
		DynamoServiceImpl dynamoServiceImpl = new DynamoServiceImpl();
		AmazonWebServiceResult result = dynamoServiceImpl.getData("product", "productId", "ABCDE1243",
				QueryTypeEnum.QUERY);
		if (result instanceof ScanResult)
			assertEquals("ABCDE1243", ((ScanResult) result).getItems().get(0).get("productId").getS());
		else
			assertEquals("ABCDE1243", ((QueryResult) result).getItems().get(0).get("productId").getS());
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testGetAllItems() {
		DynamoServiceImpl dynamoServiceImpl = new DynamoServiceImpl();
		AmazonWebServiceResult result = dynamoServiceImpl.getData("product", null, null, QueryTypeEnum.SCAN);
		if (result instanceof ScanResult)
			assertEquals(true, ((ScanResult) result).getItems().size() > 0);
		else
			assertEquals(true, ((QueryResult) result).getItems().size() > 0);
	}

	@Test
	public void testUpdateItem() throws IllegalArgumentException, IllegalAccessException, SecurityException {
		Product product = new Product();
		product.setColor("Blue");
		product.setProductId("ABCDE124113");
		product.setDescription("Jeans good quality");
		product.setMrp(1299f);
		product.setPrice(1200f);
		product.getSize().add("X");
		product.getSize().add("XL");
		product.getSize().add("XXL");
		product.getSize().add("M");
		product.getSize().add("S");
		List<String> list = Arrays.asList("productId", "price");
		DynamoServiceImpl dynamoServiceImpl = new DynamoServiceImpl();
		UpdateItemResult updateItemResult = dynamoServiceImpl.updateItem("product", list, product);
		assertEquals(200, updateItemResult.getSdkHttpMetadata().getHttpStatusCode());
	}

	@Test
	public void listTables() {
		DynamoServiceImpl dynamoServiceImpl = new DynamoServiceImpl();
		ListTablesResult listTablesResult = dynamoServiceImpl.listDynamoTables(3);
		assertEquals(true, listTablesResult.getTableNames().size() > 0);
	}

	// @Test
	public void deleteTable() {
		DynamoServiceImpl dynamoServiceImpl = new DynamoServiceImpl();
		DeleteTableResult deleteTableResult = dynamoServiceImpl.deleteTable("product");
		assertEquals(200, deleteTableResult.getSdkHttpMetadata().getHttpStatusCode());
	}

	@Test
	public void deleteTableItem() throws IllegalArgumentException, IllegalAccessException {
		DynamoServiceImpl dynamoServiceImpl = new DynamoServiceImpl();
		Product product = new Product();
		product.setProductId("ABCDE124113");
		product.setPrice(12000f);
		List<String> list = Arrays.asList("productId", "price");
		DeleteItemResult deleteItemResult = dynamoServiceImpl.deleteItem("product", list, product);
		assertEquals(200, deleteItemResult.getSdkHttpMetadata().getHttpStatusCode());
	}

}
