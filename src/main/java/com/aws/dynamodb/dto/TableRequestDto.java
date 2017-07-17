package com.aws.dynamodb.dto;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

/**
 * This will create table in dynamo database
 * @author raghugupta
 *
 */
public class TableRequestDto implements Serializable {

	private static final long serialVersionUID = -6716046085180553262L;

	private String tableName;

	private Long readCapacity;

	private Long writeCapacity;

	private Set<AttributeDetail> attributes = new HashSet<AttributeDetail>();

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Long getReadCapacity() {
		return readCapacity;
	}

	public void setReadCapacity(Long readCapacity) {
		this.readCapacity = readCapacity;
	}

	public Long getWriteCapacity() {
		return writeCapacity;
	}

	public void setWriteCapacity(Long writeCapacity) {
		this.writeCapacity = writeCapacity;
	}

	public Set<AttributeDetail> getAttributes() {
		if(attributes == null)
			attributes = new HashSet<AttributeDetail>();
		return attributes;
	}

	public void setAttributes(Set<AttributeDetail> attributes) {
		this.attributes = attributes;
	}

	public TableRequestDto(String tableName, Long readCapacity, Long writeCapacity) {
		super();
		this.tableName = tableName;
		this.readCapacity = readCapacity;
		this.writeCapacity = writeCapacity;
	}

	public static class AttributeDetail implements Serializable {

		private static final long serialVersionUID = -6567138738803435218L;
		private String attributeName;
		private KeyType keyType;
		private ScalarAttributeType scalarAttributeType;

		public String getAttributeName() {
			return attributeName;
		}

		public void setAttributeName(String attributeName) {
			this.attributeName = attributeName;
		}

		public KeyType getKeyType() {
			return keyType;
		}

		public void setKeyType(KeyType keyType) {
			this.keyType = keyType;
		}

		public ScalarAttributeType getScalarAttributeType() {
			return scalarAttributeType;
		}

		public void setScalarAttributeType(ScalarAttributeType scalarAttributeType) {
			this.scalarAttributeType = scalarAttributeType;
		}

		public AttributeDetail(String attributeName, KeyType keyType, ScalarAttributeType scalarAttributeType) {
			super();
			this.attributeName = attributeName;
			this.keyType = keyType;
			this.scalarAttributeType = scalarAttributeType;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((attributeName == null) ? 0 : attributeName.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			AttributeDetail other = (AttributeDetail) obj;
			if (attributeName == null) {
				if (other.attributeName != null)
					return false;
			} else if (!attributeName.equals(other.attributeName))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return "AttributeDetail [attributeName=" + attributeName + ", keyType=" + keyType + ", scalarAttributeType="
					+ scalarAttributeType + "]";
		}

	}

	@Override
	public String toString() {
		return "TableRequestDto [tableName=" + tableName + ", readCapacity=" + readCapacity + ", writeCapacity="
				+ writeCapacity + ", attributes=" + attributes + "]";
	}

}
