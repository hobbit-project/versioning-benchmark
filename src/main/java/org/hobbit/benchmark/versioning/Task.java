/**
 * 
 */
package org.hobbit.benchmark.versioning;

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.hobbit.core.rabbit.RabbitMQUtils;

/**
 * @author papv
 *
 */
@SuppressWarnings("serial")
public class Task implements Serializable {
	
	private String taskId;
	private String query;
	private int queryType;
	private String querySubType;
	private byte[] expectedAnswers;
	
	public Task(int queryType, String querySubType, String id, String query, byte[] expectedAnswers) {
		this.queryType = queryType;
		this.querySubType = querySubType;
		this.taskId = id;
		this.query = query;
		this.expectedAnswers = expectedAnswers;
	}
	
	public void setId(String id) {
		this.taskId = id;
	}
	
	public String getTaskId() {
		return this.taskId;
	}
	
	public void setQuery(String query) {
		this.query = query;
	}
	
	public String getQuery() {
		return this.query;
	}
	
	public void setQueryType(int queryType) {
		this.queryType = queryType;
	}
	
	public int getQueryType() {
		return this.queryType;
	}
	
	public void setQueryType(String queryType) {
		this.querySubType = queryType;
	}
	
	public String getQuerySubType() {
		return this.querySubType;
	}
	
	// the results are preceded by the query type as this information required
	// by the evaluation module. 
	public void setExpectedAnswers(byte[] res) {
		byte[] queryTypeBytes = ByteBuffer.allocate(4).putInt(queryType).array();
		this.expectedAnswers = RabbitMQUtils.writeByteArrays(queryTypeBytes, new byte[][]{res}, null);
	}
	
	public byte[] getExpectedAnswers() {
		return this.expectedAnswers;
	}
}
