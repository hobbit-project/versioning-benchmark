package org.hobbit.benchmark.versioning.components;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.jena.rdf.model.NodeIterator;
import org.hobbit.benchmark.versioning.properties.VersioningConstants;
import org.hobbit.benchmark.versioning.util.VirtuosoSystemAdapterConstants;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractBenchmarkController;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersioningBenchmarkController extends AbstractBenchmarkController {

	private static final Logger LOGGER = LoggerFactory.getLogger(VersioningBenchmarkController.class);

	private static final String DATA_GENERATOR_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/papv/versioningdatagenerator";
	private static final String TASK_GENERATOR_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/papv/versioningtaskgenerator";
	private static final String EVALUATION_MODULE_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/papv/versioningevaluationmodule";
	
	private static final String PREFIX = "http://w3id.org/hobbit/versioning-benchmark/vocab#";
	
    private String[] evalModuleEnvVariables = null;
    private String[] dataGenEnvVariables = null;
    private String[] evalStorageEnvVariables = null;
    
    private Semaphore dataGenFinishedMutex = new Semaphore(0);
    private Semaphore versionLoadedMutex = new Semaphore(0);
    
    private int numberOfDataGenerators;
    private int numOfVersions;
    private int loadedVersion = 0;
    private long prevLoadingStartedTime = 0;
    private long[] loadingTimes;
    private int[] triplesToBeLoaded;

	@Override
	public void init() throws Exception {
        LOGGER.info("Initilalizing Benchmark Controller...");
        super.init();
        
		numberOfDataGenerators = (Integer) getProperty(PREFIX + "hasNumberOfGenerators", 1);
		int datasetSize =  (Integer) getProperty(PREFIX + "datasetSizeInTriples", 1000000);
		int generatorSeed =  (Integer) getProperty(PREFIX + "generatorSeed", 0);
		numOfVersions =  (Integer) getProperty(PREFIX + "numberOfVersions", 12);
		int seedYear =  (Integer) getProperty(PREFIX + "seedYear", 2010);
		int dataGenInYears =  (Integer) getProperty(PREFIX + "generationPeriodInYears", 1);
		String serializationFormat = (String) getProperty(PREFIX + "generatedDataFormat", "n-triples");
		int subsParametersAmount = (Integer) getProperty(PREFIX + "querySubstitutionParameters", 10);
		
		loadingTimes = new long[numOfVersions];
		
		// data generators environmental values
		dataGenEnvVariables = new String[] {
				VersioningConstants.NUMBER_OF_DATA_GENERATORS + "=" + numberOfDataGenerators,
				VersioningConstants.DATA_GENERATOR_SEED + "=" + generatorSeed,
				VersioningConstants.DATASET_SIZE_IN_TRIPLES + "=" + datasetSize,
				VersioningConstants.NUMBER_OF_VERSIONS + "=" + numOfVersions,
				VersioningConstants.SEED_YEAR + "=" + seedYear,
				VersioningConstants.GENERATION_PERIOD_IN_YEARS + "=" + dataGenInYears,
				VersioningConstants.GENERATED_DATA_FORMAT + "=" + serializationFormat,
				VersioningConstants.SUBSTITUTION_PARAMETERS_AMOUNT  + "=" + subsParametersAmount
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
				VersioningConstants.QUERY_FAILURES + "=" + PREFIX + "queryFailures"
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
					if(((String) defaultValue).equals("n-triples")) {
						Properties serializationFormats = new Properties();
						try {
							serializationFormats.load(ClassLoader.getSystemResource("formats.properties").openStream());
						} catch (IOException e) {
							LOGGER.error("Exception while parsing serialization format.");
						}
						return (T) serializationFormats.getProperty(iterator.next().asResource().getLocalName());
					} else {
						return (T) iterator.next().asLiteral().getString();
					}
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
        if (command == VersioningConstants.DATA_GEN_DATA_GENERATION_FINISHED) {
        	// signal sent from data generator that all its data generated successfully
        	LOGGER.info("Signal recieved that Data Generator: X generated its data.");
        	dataGenFinishedMutex.release();
        	// TODO: remove it
        	// such information will send by the external component that will compute
        	// the gold standard, the number of data generator will received instead
        	// to replace X in the log message
        	triplesToBeLoaded = SerializationUtils.deserialize(data);
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

		// wait for all data generators to generate their data
		LOGGER.info("Waiting for all the data generators to generate their data.");
		dataGenFinishedMutex.acquire(numberOfDataGenerators);
		LOGGER.info("Signal from all Data Generators received.");
        // sends signal that all data generated and sent to system adapter successfully
		// also send the number of versions that have to be loaded
        LOGGER.info("Send signal to System Adapter that the sending of all data from Data Generators have finished.");
        sendToCmdQueue(VirtuosoSystemAdapterConstants.BULK_LOAD_DATA_GEN_FINISHED, RabbitMQUtils.writeString(Integer.toString(numOfVersions)));
        prevLoadingStartedTime = System.currentTimeMillis();
        
		// wait for the system adapter to load all versions
		LOGGER.info("Waiting for the the system to load all versions.");
		versionLoadedMutex.acquire(numOfVersions);
		
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
        			String.format(VersioningConstants.TRIPLES_TO_BE_LOADED, version) + "=" + triplesToBeLoaded[version]);
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
