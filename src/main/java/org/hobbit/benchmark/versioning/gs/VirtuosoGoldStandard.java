/**
 * 
 */
package org.hobbit.benchmark.versioning.gs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.aksw.jena_sparql_api.core.FluentQueryExecutionFactory;
import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.hobbit.benchmark.versioning.properties.VersioningConstants;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractPlatformConnectorComponent;
import org.hobbit.core.components.GeneratedDataReceivingComponent;
import org.hobbit.core.components.TaskReceivingComponent;
import org.hobbit.core.rabbit.DataHandler;
import org.hobbit.core.rabbit.DataReceiver;
import org.hobbit.core.rabbit.DataReceiverImpl;
import org.hobbit.core.rabbit.DataSender;
import org.hobbit.core.rabbit.DataSenderImpl;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author papv
 *
 */
public class VirtuosoGoldStandard extends AbstractPlatformConnectorComponent
		implements GeneratedDataReceivingComponent, TaskReceivingComponent {

	private static final Logger LOGGER = LoggerFactory.getLogger(VirtuosoGoldStandard.class);

	
    private static final int DEFAULT_MAX_PARALLEL_PROCESSED_MESSAGES = 100;
    
    /**
     * Mutex used to wait for the terminate signal.
     */
    private Semaphore terminateMutex = new Semaphore(0);

    private Semaphore startVirtuosoGSMutex = new Semaphore(0);
	
	private Semaphore dataGenFinishedMutex = new Semaphore(0);
	
	/**
     * Receiver for data coming from the data generator.
     */
	protected DataReceiver dataGenDataReceiver;
	/**
     * Receiver for tasks coming from the data generator.
     */
	protected DataReceiver dataGenTasksReceiver;
	/**
     * Sender for sending expected answers back to data generator
     */
	protected DataSender expectedAnswers2DataGenSender;
		
	/**
	 * 
	 */
	public VirtuosoGoldStandard() {
		super();
	}
	
	/**
     * Builds the virtuoso image, runs it and wait until Virtuoso server is online
     */
	@Override
    public void init() throws Exception {
        LOGGER.info("Initializing Virtuoso Component for computing Gold Standard...");
        super.init();
        
        dataGenDataReceiver = DataReceiverImpl.builder().maxParallelProcessedMsgs(DEFAULT_MAX_PARALLEL_PROCESSED_MESSAGES)
                .queue(incomingDataQueueFactory, generateSessionQueueName(VersioningConstants.DATA_GEN_DATA_2_GOLD_STD_QUEUE_NAME))
                .dataHandler(new DataHandler() {
                    public void handleData(byte[] data) {
                        receiveGeneratedData(data);
                    }
                }).build();

        dataGenTasksReceiver = DataReceiverImpl.builder().maxParallelProcessedMsgs(DEFAULT_MAX_PARALLEL_PROCESSED_MESSAGES)
                .queue(incomingDataQueueFactory, generateSessionQueueName(VersioningConstants.DATA_GEN_TASK_2_GOLD_STD_QUEUE_NAME))
                .dataHandler(new DataHandler() {
                    public void handleData(byte[] data) {
                        ByteBuffer buffer = ByteBuffer.wrap(data);
                        String taskId = RabbitMQUtils.readString(buffer);
                        byte[] taskData = RabbitMQUtils.readByteArray(buffer);
                        receiveGeneratedTask(taskId, taskData);
                    }
                }).build();
        
        expectedAnswers2DataGenSender = DataSenderImpl.builder().queue(getFactoryForOutgoingDataQueues(),
                generateSessionQueueName(VersioningConstants.GOLD_STD_2_DATA_GEN_QUEUE_NAME)).build();
        
        LOGGER.info("Virtuoso Component for computing Gold Standard initialized successfully.");
    }

	/* (non-Javadoc)
	 * @see org.hobbit.core.components.Component#run()
	 */
	public void run() throws Exception {
        sendToCmdQueue(VersioningConstants.GOLD_STD_READY_SIGNAL);
        terminateMutex.acquire();
        
        dataGenDataReceiver.closeWhenFinished();
        dataGenTasksReceiver.closeWhenFinished();
        expectedAnswers2DataGenSender.closeWhenFinished();
	}

	/* (non-Javadoc)
	 * @see org.hobbit.core.components.TaskReceivingComponent#receiveGeneratedTask(java.lang.String, byte[])
	 */
	public void receiveGeneratedTask(String taskId, byte[] data) {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.hobbit.core.components.GeneratedDataReceivingComponent#receiveGeneratedData(byte[])
	 */
	public void receiveGeneratedData(byte[] data) {
		ByteBuffer dataBuffer = ByteBuffer.wrap(data);
		// read the file path
		String receivedFilePath = RabbitMQUtils.readString(dataBuffer);
		// read the file contents
		byte[] fileContentBytes = RabbitMQUtils.readByteArray(dataBuffer);
		
		FileOutputStream fos = null;
		try {
			File outputFile = new File(receivedFilePath);
			fos = FileUtils.openOutputStream(outputFile, false);
			IOUtils.write(fileContentBytes, fos);
			fos.close();
			// test
			BufferedReader reader = new BufferedReader(new FileReader(receivedFilePath));
			int lines = 0;
			while (reader.readLine() != null) lines++;
			reader.close();
			LOGGER.info(receivedFilePath + " (" + (double) new File(receivedFilePath).length() / 1000 + " KB) received from Data Generator with " + lines + " lines.");

		} catch (FileNotFoundException e) {
			LOGGER.error("Exception while creating/opening files to write received data.", e);
		} catch (IOException e) {
			LOGGER.error("Exception while writing data file", e);
		}		

	}
	
	@Override
    public void receiveCommand(byte command, byte[] data) {
		if (command == VersioningConstants.DATA_GEN_DATA_GENERATION_STARTED) {
			int numberOfDataGenerators = Integer.parseInt(RabbitMQUtils.readString(data));
			try {
				// wait until data from all data generators have been received
				dataGenFinishedMutex.acquire(numberOfDataGenerators);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			LOGGER.info("Received command from BC, num of datagens " + numberOfDataGenerators);
		} else if (command == VersioningConstants.DATA_GEN_DATA_GENERATION_FINISHED) {
			startVirtuosoGSMutex.release();
		}
    }

}
