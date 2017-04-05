/**
 * 
 */
package org.hobbit.benchmark.versioning;

import java.io.Serializable;

/**
 * @author papv
 *
 */
@SuppressWarnings("serial")
public class Task implements Serializable {
	
	/**
	 * taskType denotes the type of the task and may have the following values:
	 * 1 - ingestion task
	 * 2 - storage space task
	 * 3 - SPARQL query task
	 */
	private String taskType;
	private String taskId;
	private String query;
	private byte[] expectedAnswers;
	
	public Task(String taskType, String id, String query, byte[] expectedAnswers) {
		this.taskType = taskType;
		this.taskId = id;
		this.query = query;
		this.expectedAnswers = expectedAnswers;
	}
	
	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}
	
	public String getTaskType() {
		return this.taskType;
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
	
	public void setExpectedAnswers(byte[] res) {
		this.expectedAnswers =  res;
	}
	
	public byte[] getExpectedAnswers() {
		return this.expectedAnswers;
	}
}
