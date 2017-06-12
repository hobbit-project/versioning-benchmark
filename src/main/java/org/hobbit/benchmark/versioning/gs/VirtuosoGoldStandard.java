/**
 * 
 */
package org.hobbit.benchmark.versioning.gs;

import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.aksw.jena_sparql_api.core.FluentQueryExecutionFactory;
import org.aksw.jena_sparql_api.core.QueryExecutionFactory;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.ResultSet;
import org.hobbit.benchmark.versioning.properties.VersioningConstants;
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

	private Semaphore startVirtuosoGSMutex = new Semaphore(0);
	
	private Semaphore dataGenFinishedMutex = new Semaphore(0);
	
    private static final int DEFAULT_MAX_PARALLEL_PROCESSED_MESSAGES = 100;

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
        
        String[] envVariablesVirtuoso = new String[] { 
        		"SPARQL_UPDATE=true",
        		"DEFAULT_GRAPH=http://www.virtuoso-graph.com/" };
        
        String virtuosoContName = createContainer("tenforce/virtuoso:latest", envVariablesVirtuoso);
        
        QueryExecutionFactory qef = FluentQueryExecutionFactory
                .http("http://" + virtuosoContName + ":8890/sparql")
                .config()
                .withPagination(50000)
                .end()
                .create();


        ResultSet testResults = null;
        while (testResults == null) {
            LOGGER.info("Using " + "http://" + virtuosoContName + ":8890/sparql" + " to run test select query");

            QueryExecution qe = qef.createQueryExecution("SELECT * { ?s a <http://ex.org/foo/bar> } LIMIT 1");
            try {
                TimeUnit.SECONDS.sleep(2);
                testResults = qe.execSelect();
            } catch (Exception e) {
            } finally {
                qe.close();
            }
        }
        qef.close();
        
        LOGGER.info("Virtuoso Component initialized successfully.");
    }

	/* (non-Javadoc)
	 * @see org.hobbit.core.components.Component#run()
	 */
	public void run() throws Exception {
		sendToCmdQueue(VersioningConstants.VIRTUOSO_GS_READY_SIGNAL);
		// Wait for the start message
//		startVirtuosoGSMutex.acquire();
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
		// TODO Auto-generated method stub

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
