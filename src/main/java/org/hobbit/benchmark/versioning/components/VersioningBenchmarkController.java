package org.hobbit.benchmark.versioning.components;

import org.apache.jena.rdf.model.NodeIterator;
import org.hobbit.benchmark.versioning.properties.VersioningConstants;
import org.hobbit.core.Commands;
import org.hobbit.core.components.AbstractBenchmarkController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersioningBenchmarkController extends AbstractBenchmarkController {

	private static final Logger LOGGER = LoggerFactory.getLogger(VersioningBenchmarkController.class);

//	private static final String DATA_GENERATOR_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/papv/versioningdatagenerator";
//	private static final String TASK_GENERATOR_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/papv/versioningtaskgenerator";
//	private static final String EVALUATION_MODULE_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/papv/versioningevaluationmodule";
	private static final String DATA_GENERATOR_CONTAINER_IMAGE = "versioning_data-generator";
	private static final String TASK_GENERATOR_CONTAINER_IMAGE = "versioning_task-generator";
	private static final String EVALUATION_MODULE_CONTAINER_IMAGE = "versioning_evaluation-module";

	@Override
	public void init() throws Exception {
        LOGGER.info("Initilalizing Benchmark Controller...");
		super.init();
        
		int numberOfDataGenerators = (Integer) getProperty("http://example.org/hasNumberOfGenerators", 1);
		int datasetSize =  (Integer) getProperty("http://example.org/datasetSizeInTriples", 1000000);
		int generatorSeed =  (Integer) getProperty("http://example.org/generatorSeed", 0);
		int numOfVersions =  (Integer) getProperty("http://example.org/numberOfVersions", 12);
		int seedYear =  (Integer) getProperty("http://example.org/seedYear", 2010);
		int dataGenInYears =  (Integer) getProperty("http://example.org/generationPeriodInYears", 1);
		String generatedDataDir = (String) getProperty("http://example.org/generatedDataDir", "generated");
		String serializationFormat = (String) getProperty("http://example.org/generatedDataFormat", "n-triples");
		int subsParametersAmount = (Integer) getProperty("http://example.org/querySubstitutionParameters", 10);
		
		// data generators environmental values
		String[] envVariablesDataGenerator = new String[] {
				VersioningConstants.NUMBER_OF_DATA_GENERATORS + "=" + numberOfDataGenerators,
				VersioningConstants.DATA_GENERATOR_SEED + "=" + generatorSeed,
				VersioningConstants.DATASET_SIZE_IN_TRIPLES + "=" + datasetSize,
				VersioningConstants.NUMBER_OF_VERSIONS + "=" + numOfVersions,
				VersioningConstants.SEED_YEAR + "=" + seedYear,
				VersioningConstants.GENERATION_PERIOD_IN_YEARS + "=" + dataGenInYears,
				VersioningConstants.GENERATED_DATA_DIR + "=" + generatedDataDir,
				VersioningConstants.GENERATED_DATA_FORMAT + "=" + serializationFormat,
				VersioningConstants.SUBSTITUTION_PARAMETERS_AMOUNT  + "=" + subsParametersAmount
		};
		
		// Create data generators
		createDataGenerators(DATA_GENERATOR_CONTAINER_IMAGE, numberOfDataGenerators, envVariablesDataGenerator);
		LOGGER.info("Data Generators created successfully.");

		// Create task generators
		createTaskGenerators(TASK_GENERATOR_CONTAINER_IMAGE, 1, new String[] {} );
		LOGGER.info("Task Generators created successfully.");

		// Create evaluation storage
		createEvaluationStorage();
		LOGGER.info("Evaluation Storage created successfully.");
		
		waitForComponentsToInitialize();
		LOGGER.info("All components initilized.");
	}
	
	/**
     * A generic method for loading parameters from the benchmark parameter model
     * 
     * @param property
     *            the property that we want to load
     * @param defaultValue
     *            the default value that will be used in case of an error while
     *            loading the property
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
        waitForSystemToFinish();
        LOGGER.info("System terminated.");
        
        // create the evaluation module
        String[] envVariablesEvaluationModule = new String[] { };
        createEvaluationModule(EVALUATION_MODULE_CONTAINER_IMAGE, envVariablesEvaluationModule);
        
        // wait for the evaluation to finish
        LOGGER.info("Waiting for the evaluation to finish.");
        waitForEvalComponentsToFinish();
        LOGGER.info("Evaluation finished.");

        // Send the resultModul to the platform controller and terminate
        sendResultModel(this.resultModel);
        LOGGER.info("Evaluated results sent to the platform controller.");
	}
}
