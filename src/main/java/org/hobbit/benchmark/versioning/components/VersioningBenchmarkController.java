package org.hobbit.benchmark.versioning.components;

import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.jena.rdf.model.NodeIterator;
import org.hobbit.benchmark.versioning.properties.VersioningConstants;
import org.hobbit.benchmark.versioning.util.VirtuosoSystemAdapterConstants;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractBenchmarkController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersioningBenchmarkController extends AbstractBenchmarkController {

	private static final Logger LOGGER = LoggerFactory.getLogger(VersioningBenchmarkController.class);

	private static final String DATA_GENERATOR_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/papv/versioningdatagenerator:2.0";
	private static final String TASK_GENERATOR_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/papv/versioningtaskgenerator:2.0";
	private static final String EVALUATION_MODULE_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/papv/versioningevaluationmodule:2.0";
	
	private static final String PREFIX = "http://w3id.org/hobbit/versioning-benchmark/vocab#";
	
    private String[] evalModuleEnvVariables = null;
    private String[] dataGenEnvVariables = null;
    private String[] evalStorageEnvVariables = null;
    
    private Semaphore versionSentMutex = new Semaphore(0);
    private Semaphore versionLoadedMutex = new Semaphore(0);
    
    private int numberOfDataGenerators;
    private int numOfVersions;
    private int loadedVersion = 0;
    private long prevLoadingStartedTime = 0;
    private long[] loadingTimes;
    private AtomicIntegerArray triplesToBeLoaded;
    private AtomicInteger numberOfMessages = new AtomicInteger(0);

	@Override
	public void init() throws Exception {
        LOGGER.info("Initilalizing Benchmark Controller...");
        super.init();
        
		numberOfDataGenerators = (Integer) getProperty(PREFIX + "hasNumberOfGenerators", 1);
		int v0Size =  (Integer) getProperty(PREFIX + "v0SizeInTriples", 1000000);
		int generatorSeed =  (Integer) getProperty(PREFIX + "generatorSeed", 0);
		numOfVersions =  (Integer) getProperty(PREFIX + "numberOfVersions", 12);
		int insRatio = (Integer) getProperty(PREFIX + "versionInsertionRatio", 5);
		int delRatio = (Integer) getProperty(PREFIX + "versionDeletionRatio", 3);
		
		loadingTimes = new long[numOfVersions];
		triplesToBeLoaded = new AtomicIntegerArray(numOfVersions);
		
		// data generators environmental values
		dataGenEnvVariables = new String[] {
				VersioningConstants.NUMBER_OF_DATA_GENERATORS + "=" + numberOfDataGenerators,
				VersioningConstants.DATA_GENERATOR_SEED + "=" + generatorSeed,
				VersioningConstants.V0_SIZE_IN_TRIPLES + "=" + v0Size,
				VersioningConstants.NUMBER_OF_VERSIONS + "=" + numOfVersions,
				VersioningConstants.VERSION_INSERTION_RATIO + "=" + insRatio,
				VersioningConstants.VERSION_DELETION_RATIO  + "=" + delRatio
		};
		
		// evaluation module environmental values
		evalModuleEnvVariables = new String[] {
				VersioningConstants.INITIAL_VERSION_INGESTION_SPEED + "=" + PREFIX + "initialVersionIngestionSpeed",
				VersioningConstants.AVG_APPLIED_CHANGES_PS + "=" + PREFIX + "avgAppliedChangesPS",
				VersioningConstants.STORAGE_COST + "=" + PREFIX + "storageCost",
				VersioningConstants.QT_1_AVG_EXEC_TIME + "=" + PREFIX + "queryType1AvgExecTime",
				VersioningConstants.QT_2_AVG_EXEC_TIME + "=" + PREFIX + "queryType2AvgExecTime",
				VersioningConstants.QT_3_AVG_EXEC_TIME + "=" + PREFIX + "queryType3AvgExecTime",
				VersioningConstants.QT_4_AVG_EXEC_TIME + "=" + PREFIX + "queryType4AvgExecTime",
				VersioningConstants.QT_5_AVG_EXEC_TIME + "=" + PREFIX + "queryType5AvgExecTime",
				VersioningConstants.QT_6_AVG_EXEC_TIME + "=" + PREFIX + "queryType6AvgExecTime",
				VersioningConstants.QT_7_AVG_EXEC_TIME + "=" + PREFIX + "queryType7AvgExecTime",
				VersioningConstants.QT_8_AVG_EXEC_TIME + "=" + PREFIX + "queryType8AvgExecTime",
				VersioningConstants.QUERY_FAILURES + "=" + PREFIX + "queryFailures",
				VersioningConstants.QUERIES_PER_SECOND + "=" + PREFIX + "queriesPerSecond"
		};
		
		evalStorageEnvVariables = ArrayUtils.add(DEFAULT_EVAL_STORAGE_PARAMETERS,
                Constants.RABBIT_MQ_HOST_NAME_KEY + "=" + this.rabbitMQHostName);
		evalStorageEnvVariables = ArrayUtils.add(evalStorageEnvVariables, "ACKNOWLEDGEMENT_FLAG=true");
		
		// Create data generators
		createDataGenerators(DATA_GENERATOR_CONTAINER_IMAGE, numberOfDataGenerators, dataGenEnvVariables);
		LOGGER.info("Data Generators created successfully.");

		// Create task generators
		createTaskGenerators(TASK_GENERATOR_CONTAINER_IMAGE, 1, new String[] {} );
		LOGGER.info("Task Generators created successfully.");

		// Create evaluation storage
		createEvaluationStorage(DEFAULT_EVAL_STORAGE_IMAGE, evalStorageEnvVariables);
		LOGGER.info("Evaluation Storage created successfully.");
		
		waitForComponentsToInitialize();
		LOGGER.info("All components initilized.");
	}
	
	/**
     * A generic method for loading parameters from the benchmark parameter model
     * 
     * @param property		the property that we want to load
     * @param defaultValue	the default value that will be used in case of an error while loading the property
     * @return				the value of requested parameter
     */
	@SuppressWarnings("unchecked")
	private <T> T getProperty(String property, T defaultValue) {
		T propertyValue = null;
		NodeIterator iterator = benchmarkParamModel
				.listObjectsOfProperty(benchmarkParamModel
		        .getProperty(property));

		if (iterator.hasNext()) {
			try {
				if (defaultValue instanceof String) {
					return (T) iterator.next().asLiteral().getString();
				} else if (defaultValue instanceof Integer) {
					return (T) ((Integer) iterator.next().asLiteral().getInt());
				} else if (defaultValue instanceof Long) {
					return (T) ((Long) iterator.next().asLiteral().getLong());
				} else if (defaultValue instanceof Double) {
					return (T) ((Double) iterator.next().asLiteral().getDouble());
				}
            } catch (Exception e) {
            	LOGGER.error("Exception while parsing parameter.");
            }
		} else {
			LOGGER.info("Couldn't get property '" + property + "' from the parameter model. Using '" + defaultValue + "' as a default value.");
			propertyValue = defaultValue;
		}
		return propertyValue;
	}
	
	@Override
    public void receiveCommand(byte command, byte[] data) {
        if (command == VersioningConstants.DATA_GEN_VERSION_DATA_SENT) {
        	// TODO: 
        	// triplesToBeLoaded information have to be sent by the external component 
        	// computing the gold standard. Only the number of messages and data generator's
        	// id will be sent with DATA_GEN_VERSION_SENT command
        	ByteBuffer buffer = ByteBuffer.wrap(data);
        	int triplesNum = buffer.getInt();
            int dataGeneratorId = buffer.getInt();
            int dataGenNumOfMessages = buffer.getInt();
        	triplesToBeLoaded.addAndGet(loadedVersion, triplesNum);
        	numberOfMessages.addAndGet(dataGenNumOfMessages);
        	
        	// signal sent from data generator that all its data generated successfully
        	LOGGER.info("Recieved signal from Data Generator " + dataGeneratorId + " that all data (#" + dataGenNumOfMessages + ") of version " + loadedVersion + " successfully sent to System Adapter.");
        	versionSentMutex.release();
	    } else if (command == VirtuosoSystemAdapterConstants.BULK_LOADING_DATA_FINISHED) {
            // signal sent from system adapter that a version loaded successfully
        	long currTimeMillis = System.currentTimeMillis();
        	long versionLoadingTime = currTimeMillis - prevLoadingStartedTime;
        	loadingTimes[loadedVersion++] = versionLoadingTime;
        	prevLoadingStartedTime = currTimeMillis;
        	versionLoadedMutex.release();
        } 
        super.receiveCommand(command, data);
    }
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.hobbit.core.components.AbstractBenchmarkController#executeBenchmark()
	 */
	@Override
	protected void executeBenchmark() throws Exception {
		// give the start signals
        sendToCmdQueue(Commands.TASK_GENERATOR_START_SIGNAL);
        sendToCmdQueue(Commands.DATA_GENERATOR_START_SIGNAL);
		LOGGER.info("Start signals sent to Data and Task Generators");

		// iterate through different versions starting from version 0
		for (int v=0; v<numOfVersions; v++) {			
			// wait for all data generators to sent data of version v to system adapter
			LOGGER.info("Waiting for all data generators to send data of version " + v + " to system adapter.");
			versionSentMutex.acquire(numberOfDataGenerators);
			LOGGER.info("Signal from all data generators received.");
			
			// Send signal that all data, generated and sent to system adapter successfully.
			// The number of messages along with a flag is also sent
			LOGGER.info("Send signal to System Adapter that the sending of all data of version " + v + " from Data Generators have finished.");
			ByteBuffer buffer = ByteBuffer.allocate(5);
	        buffer.putInt(numberOfMessages.get());
	        buffer.put(v == numOfVersions - 1 ? (byte) 1 : (byte) 0);
	        prevLoadingStartedTime = System.currentTimeMillis();
	        sendToCmdQueue(VirtuosoSystemAdapterConstants.BULK_LOAD_DATA_GEN_FINISHED, buffer.array());
	        numberOfMessages.set(0);
	        
	        LOGGER.info("Waiting for the system to load data of version " + v);
			versionLoadedMutex.acquire();
		}

        // wait for the data generators to finish their work
        LOGGER.info("Waiting for the data generators to finish their work.");
        waitForDataGenToFinish();
        LOGGER.info("Data generators finished.");

        // wait for the task generators to finish their work
        LOGGER.info("Waiting for the task generators to finish their work.");
        waitForTaskGenToFinish();
        LOGGER.info("Task generators finished.");

        // wait for the system to terminate
        LOGGER.info("Waiting for the system to terminate.");
        waitForSystemToFinish(1000 * 60 * 5);
        LOGGER.info("System terminated.");
        
        // pass the number of versions composing the dataset to the environment 
        // variables of the evaluation module
        evalModuleEnvVariables = ArrayUtils.add(evalModuleEnvVariables, VersioningConstants.TOTAL_VERSIONS + "=" + numOfVersions);
        
        // pass the loading times and the number of triples that have to be loaded to 
        // the environment variables of the evaluation module, so that it can compute
        // the ingestion and applied changes speeds
        for(int version=0; version<numOfVersions; version++) {
        	evalModuleEnvVariables = ArrayUtils.add(evalModuleEnvVariables, 
        			String.format(VersioningConstants.TRIPLES_TO_BE_LOADED, version) + "=" + triplesToBeLoaded.get(version));
        	evalModuleEnvVariables = ArrayUtils.add(evalModuleEnvVariables, 
        			String.format(VersioningConstants.LOADING_TIMES, version) + "=" + loadingTimes[version]);
        }
        // create the evaluation module
        createEvaluationModule(EVALUATION_MODULE_CONTAINER_IMAGE, evalModuleEnvVariables);
        
        // wait for the evaluation to finish
        LOGGER.info("Waiting for the evaluation to finish.");
        waitForEvalComponentsToFinish();
        LOGGER.info("Evaluation finished.");

        // Send the resultModul to the platform controller and terminate
        sendResultModel(this.resultModel);
        LOGGER.info("Evaluated results sent to the platform controller.");
	}
}
