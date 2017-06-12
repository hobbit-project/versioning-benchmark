package org.hobbit.benchmark.versioning.components;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.lang3.ArrayUtils;
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
	private static final String VIRTUOSO_GS_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/papv/versioningvirtuosogs";
	
	private static final String PREFIX = "http://w3id.org/hobbit/versioning-benchmark/vocab#";
	
    private ArrayList<String> evalModuleEnvVariables = new ArrayList<String>();
    private String[] dataGenEnvVariables = null;
    private String[] evalStorageEnvVariables = null;
    private int numberOfDataGenerators = 0;

	@Override
	public void init() throws Exception {
        LOGGER.info("Initilalizing Benchmark Controller...");
        super.init();
        
		numberOfDataGenerators = (Integer) getProperty(PREFIX + "hasNumberOfGenerators", 1);
		int datasetSize =  (Integer) getProperty(PREFIX + "datasetSizeInTriples", 1000000);
		int generatorSeed =  (Integer) getProperty(PREFIX + "generatorSeed", 0);
		int numOfVersions =  (Integer) getProperty(PREFIX + "numberOfVersions", 12);
		int seedYear =  (Integer) getProperty(PREFIX + "seedYear", 2010);
		int dataGenInYears =  (Integer) getProperty(PREFIX + "generationPeriodInYears", 1);
		String serializationFormat = (String) getProperty(PREFIX + "generatedDataFormat", "n-triples");
		int subsParametersAmount = (Integer) getProperty(PREFIX + "querySubstitutionParameters", 10);
		
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
		evalModuleEnvVariables.add(VersioningConstants.INITIAL_VERSION_INGESTION_SPEED + "=" + PREFIX + "initialVersionIngestionSpeed");
		evalModuleEnvVariables.add(VersioningConstants.AVG_APPLIED_CHANGES_PS + "=" + PREFIX + "avgAppliedChangesPS");
		evalModuleEnvVariables.add(VersioningConstants.STORAGE_COST + "=" + PREFIX + "storageCost");
		evalModuleEnvVariables.add(VersioningConstants.QT_1_AVG_EXEC_TIME + "=" + PREFIX + "queryType1AvgExecTime");
		evalModuleEnvVariables.add(VersioningConstants.QT_2_AVG_EXEC_TIME + "=" + PREFIX + "queryType2AvgExecTime");
		evalModuleEnvVariables.add(VersioningConstants.QT_3_AVG_EXEC_TIME + "=" + PREFIX + "queryType3AvgExecTime");
		evalModuleEnvVariables.add(VersioningConstants.QT_4_AVG_EXEC_TIME + "=" + PREFIX + "queryType4AvgExecTime");
		evalModuleEnvVariables.add(VersioningConstants.QT_5_AVG_EXEC_TIME + "=" + PREFIX + "queryType5AvgExecTime");
		evalModuleEnvVariables.add(VersioningConstants.QT_6_AVG_EXEC_TIME + "=" + PREFIX + "queryType6AvgExecTime");
		evalModuleEnvVariables.add(VersioningConstants.QT_7_AVG_EXEC_TIME + "=" + PREFIX + "queryType7AvgExecTime");
		evalModuleEnvVariables.add(VersioningConstants.QT_8_AVG_EXEC_TIME + "=" + PREFIX + "queryType8AvgExecTime");
		evalModuleEnvVariables.add(VersioningConstants.QUERY_FAILURES + "=" + PREFIX + "queryFailures");
		
		
		evalStorageEnvVariables = ArrayUtils.add(DEFAULT_EVAL_STORAGE_PARAMETERS,
                Constants.RABBIT_MQ_HOST_NAME_KEY + "=" + this.rabbitMQHostName);
		evalStorageEnvVariables = ArrayUtils.add(evalStorageEnvVariables, "ACKNOWLEDGEMENT_FLAG=true");
		
		// Create data generators
		createDataGenerators(DATA_GENERATOR_CONTAINER_IMAGE, numberOfDataGenerators, dataGenEnvVariables);
		LOGGER.info("Data Generators created successfully.");

		// Create task generators
		createTaskGenerators(TASK_GENERATOR_CONTAINER_IMAGE, 1, new String[] {} );
		LOGGER.info("Task Generators created successfully.");
		
		// Create component responsible for computing the gold standard
		createContainer(VIRTUOSO_GS_CONTAINER_IMAGE, new String[] {});
		LOGGER.info("Virtuoso Gold Standard created successfully.");

		// Create evaluation storage
		createEvaluationStorage(DEFAULT_EVAL_STORAGE_IMAGE, evalStorageEnvVariables);
		LOGGER.info("Evaluation Storage created successfully.");
		
		waitForComponentsToInitialize();
		LOGGER.info("All components initilized.");
	}
	
	@Override
	protected void waitForComponentsToInitialize() {
		super.waitForComponentsToInitialize();
        LOGGER.debug("Waiting for Virtuoso Gold Standard to be ready.");
        try {
            dataGenReadyMutex.acquire(dataGenContainerIds.size());
        } catch (InterruptedException e) {
            String errorMsg = "Interrupted while waiting for the virtuoso gold standard to be ready.";
            LOGGER.error(errorMsg);
            throw new IllegalStateException(errorMsg, e);
        }    
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
        sendToCmdQueue(VersioningConstants.DATA_GEN_DATA_GENERATION_STARTED, RabbitMQUtils.writeString(Integer.toString(numberOfDataGenerators)));
		LOGGER.info("Start signals sent to Data and Task Generators");

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
        
        // create the evaluation module
        createEvaluationModule(EVALUATION_MODULE_CONTAINER_IMAGE, evalModuleEnvVariables.toArray(new String[evalModuleEnvVariables.size()]));
        
        // wait for the evaluation to finish
        LOGGER.info("Waiting for the evaluation to finish.");
        waitForEvalComponentsToFinish();
        LOGGER.info("Evaluation finished.");

        // Send the resultModul to the platform controller and terminate
        sendResultModel(this.resultModel);
        LOGGER.info("Evaluated results sent to the platform controller.");
	}
}
