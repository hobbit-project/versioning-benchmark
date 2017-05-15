package org.hobbit.benchmark.versioning.components;

import java.io.IOException;

import org.apache.commons.lang3.SerializationUtils;
import org.hobbit.benchmark.versioning.Task;
import org.hobbit.benchmark.versioning.util.VirtuosoSystemAdapterConstants;
import org.hobbit.core.components.AbstractSequencingTaskGenerator;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersioningTaskGenerator extends AbstractSequencingTaskGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(VersioningTaskGenerator.class);
   		
	@Override
    public void init() throws Exception {
        LOGGER.info("Initializing Task Generator...");
		super.init();
        LOGGER.info("Task Generator initialized successfully.");
    }
	
	@Override
	/**
	 * The following method is called when method sendDataToTaskGenerator of
	 * Data Generators is called. In practice an already generated task along with
	 * its expected answers sent here.
	 * @param data represents the already generated tasks and their answers, which 
	 * have previously generated from the data generator
	 */
	protected void generateTask(byte[] data) {		
		try {
			// receive the generated task
			Task task = (Task) SerializationUtils.deserialize(data);
			String taskId = task.getTaskId();
			String taskType = task.getTaskType();
			String taskQuery = task.getQuery();
			LOGGER.info("Task " + taskId + " received from Data Generator");
	
			if(taskType.equals("2")) {
				// as task generator extends the AbstractSequencingTaskGenerator if task of 
				// type 2 is received all tasks of type 1 (loading) have finished
				try {
					sendToCmdQueue(VirtuosoSystemAdapterConstants.BULK_LOADING_DATA_FINISHED);
					LOGGER.info("Signal that loading phase finished sent to System Adapter");
				} catch (IOException e1) {
					LOGGER.error("An error occured while sending message to cmd queue.", e1);
				}
			}
			long timestamp = System.currentTimeMillis();
			byte[][] taskDataArray = new byte[2][];
			taskDataArray[0] = RabbitMQUtils.writeString(taskType);
			taskDataArray[1] = RabbitMQUtils.writeString(taskQuery);

			// Send the task to the system
			byte[] taskData = RabbitMQUtils.writeByteArrays(taskDataArray);
	        sendTaskToSystemAdapter(taskId, taskData);
			LOGGER.info("Task " + taskId + " sent to System Adapter.");
	
			// Send the expected answers to the evaluation storage
			// (note that, storage space task has no expected answers)
			if(taskType.equals("2")) {
				sendTaskToEvalStorage(taskId, timestamp, new byte[] {});
			} else {
		        sendTaskToEvalStorage(taskId, timestamp, task.getExpectedAnswers());
			}
			LOGGER.info("Expected answers of task " + taskId + " sent to Evaluation Storage.");

	    } catch (Exception e) {
			LOGGER.error("Exception caught while reading the tasks and their expected answers", e);
		}
	}
	
	@Override
    public void close() throws IOException {
		LOGGER.info("Closing Task Generator...");
		super.close();
		LOGGER.info("Task Generator closed successfully.");
	}
}
