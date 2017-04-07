package org.hobbit.benchmark.versioning.components;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.hobbit.benchmark.versioning.properties.VersioningConstants;
import org.hobbit.benchmark.versioning.systems.VirtuosoSystemAdapter;
import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractEvaluationModule;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.vocab.HOBBIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersioningEvaluationModule extends AbstractEvaluationModule {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(VersioningEvaluationModule.class);

    private Model finalModel = ModelFactory.createDefaultModel();
    
    private Property INITIAL_VERSION_INGESTION_SPEED = null;
    private Property AVG_APPLIED_CHANGES_PS = null;
    private Property STORAGE_COST = null;
    private Property QT_1_AVG_EXEC_TIME = null;
    private Property QT_2_AVG_EXEC_TIME = null;
    private Property QT_3_AVG_EXEC_TIME = null;
    private Property QT_4_AVG_EXEC_TIME = null;
    private Property QT_5_AVG_EXEC_TIME = null;
    private Property QT_6_AVG_EXEC_TIME = null;
    private Property QT_7_AVG_EXEC_TIME = null;
    private Property QT_8_AVG_EXEC_TIME = null;

    private HashMap<Integer,Integer> changesPerVersion = new HashMap<Integer,Integer>(); 
    
    private AtomicLong ingestionTasksErrors;
    private AtomicLong storageSpaceTasksErrors;
    private AtomicLong queryPerformanceTasksErrors;
    
	private double initialVersionIngestionSpeed = 0;
	private double avgAppliedChangesPS = 0;
	private double totalAppliedChangesPS = 0;
	private double storageCost = 0;
	private double queryType1AvgExecTime = 0;
	private double queryType2AvgExecTime = 0;
	private double queryType3AvgExecTime = 0;
	private double queryType4AvgExecTime = 0;
	private double queryType5AvgExecTime = 0;
	private double queryType6AvgExecTime = 0;
	private double queryType7AvgExecTime = 0;
	private double queryType8AvgExecTime = 0;
	
	private long queryType1Sum = 0;
	private long queryType2Sum = 0;
	private long queryType3Sum = 0;
	private long queryType4Sum = 0;
	private long queryType5Sum = 0;
	private long queryType6Sum = 0;
	private long queryType7Sum = 0;
	private long queryType8Sum = 0;
	
	
	private int queryType1Count = 0;
	private int queryType2Count = 0;
	private int queryType3Count = 0;
	private int queryType4Count = 0;
	private int queryType5Count = 0;
	private int queryType6Count = 0;
	private int queryType7Count = 0;
	private int queryType8Count = 0;

	@Override
    public void init() throws Exception {
        // Always init the super class first!
        super.init();
        
        Map<String, String> env = System.getenv();
        INITIAL_VERSION_INGESTION_SPEED = initFinalModelFromEnv(env, VersioningConstants.INITIAL_VERSION_INGESTION_SPEED);
        AVG_APPLIED_CHANGES_PS = initFinalModelFromEnv(env, VersioningConstants.AVG_APPLIED_CHANGES_PS);
        STORAGE_COST = initFinalModelFromEnv(env, VersioningConstants.STORAGE_COST);
        QT_1_AVG_EXEC_TIME = initFinalModelFromEnv(env, VersioningConstants.QT_1_AVG_EXEC_TIME);
        QT_2_AVG_EXEC_TIME = initFinalModelFromEnv(env, VersioningConstants.QT_2_AVG_EXEC_TIME);
        QT_3_AVG_EXEC_TIME = initFinalModelFromEnv(env, VersioningConstants.QT_3_AVG_EXEC_TIME);
        QT_4_AVG_EXEC_TIME = initFinalModelFromEnv(env, VersioningConstants.QT_4_AVG_EXEC_TIME);
        QT_5_AVG_EXEC_TIME = initFinalModelFromEnv(env, VersioningConstants.QT_5_AVG_EXEC_TIME);
        QT_6_AVG_EXEC_TIME = initFinalModelFromEnv(env, VersioningConstants.QT_6_AVG_EXEC_TIME);
        QT_7_AVG_EXEC_TIME = initFinalModelFromEnv(env, VersioningConstants.QT_7_AVG_EXEC_TIME);
        QT_8_AVG_EXEC_TIME = initFinalModelFromEnv(env, VersioningConstants.QT_8_AVG_EXEC_TIME);
    }
	
	/**
     * Initialize evaluation module parameters from environment variables
     * 
     * @param env		a map of all available environment variables
     * @param parameter	the property that we want to get
     */
	@SuppressWarnings("unchecked")
	private Property initFinalModelFromEnv(Map<String, String> env, String parameter) {
		if (!env.containsKey(parameter)) {
			LOGGER.error(
					"Environment variable \"" + parameter + "\" is not set. Aborting.");
            throw new IllegalArgumentException(
            		"Environment variable \"" + parameter + "\" is not set. Aborting.");
        }
		Property property =  finalModel.createProperty(env.get(parameter));
		return property;
	}

	@Override
	protected void evaluateResponse(byte[] expectedData, byte[] receivedData, long taskSentTimestamp,
			long responseReceivedTimestamp) throws Exception {
		
		ByteBuffer expectedBuffer = ByteBuffer.wrap(expectedData);
		ByteBuffer receivedBuffer = ByteBuffer.wrap(receivedData);
		
		// get the task type
		String taskType = RabbitMQUtils.readString(receivedBuffer);
				
		switch (Integer.parseInt(taskType)) {
			case 1:
				LOGGER.info("Evaluating ingestion time task's response...");
				// get the loaded version
				int version = Integer.parseInt(RabbitMQUtils.readString(expectedBuffer));
				// get the triples that had to be loaded by the system
				int expectedLoadedTriples = Integer.parseInt(RabbitMQUtils.readString(expectedBuffer));
				
				// get the changes that successfully applied by the system
				int loadedTriples = Integer.parseInt(RabbitMQUtils.readString(receivedBuffer));
				// get the time, system requires to load the aformentioned triples
				long loadingTime = Long.parseLong(RabbitMQUtils.readString(receivedBuffer));
				
				if(loadedTriples != expectedLoadedTriples) {
					LOGGER.error(String.format("Total of %,d triples exist in the database, instead "
							+ "of %,d after loading of version %d", loadedTriples, expectedLoadedTriples, version));
					ingestionTasksErrors.incrementAndGet();
				} else {
					if(version == 0) {
						changesPerVersion.put(version, loadedTriples);
						// speed has to computed in seconds (triples/second)
						initialVersionIngestionSpeed =  (double) loadedTriples / (loadingTime / 1000);
					} else {
						int currentAppliedChanges = loadedTriples - changesPerVersion.get(version - 1);
						changesPerVersion.put(version, currentAppliedChanges);
						totalAppliedChangesPS += (double) currentAppliedChanges / (loadingTime / 1000);
					}
				}
				
				LOGGER.info("Ingestion task's response - Total triples after loading version " + version + ": " +
						loadedTriples + " of " + expectedLoadedTriples + ", loading time: " + loadingTime + " ms.");
				break;
			case 2:
				LOGGER.info("Evaluating storage space task's response...");
				// get the disk space used in KB
				storageCost = Long.parseLong(RabbitMQUtils.readString(receivedBuffer)) / 1000;
				LOGGER.info("Response: " + storageCost + " KB.");
				break;
			case 3:	
				LOGGER.info("Evaluating response of query performance task...");

				// get the expected result's row number
				int expectedResultsNum = Integer.parseInt(RabbitMQUtils.readString(expectedBuffer));
				// get the expected results
				InputStream inExpected = new ByteArrayInputStream(
						RabbitMQUtils.readString(expectedBuffer).getBytes(StandardCharsets.UTF_8));
//				ResultSet received = ResultSetFactory.fromJSON(inExpected);
				
				// get the query type
				int queryType = Integer.parseInt(RabbitMQUtils.readString(receivedBuffer));
				// get its execution time
				long execTime = Long.parseLong(RabbitMQUtils.readString(receivedBuffer));
				// get the results row count
				int resultRowCount = Integer.parseInt(RabbitMQUtils.readString(receivedBuffer));
				// get the received results
				InputStream inReceived = new ByteArrayInputStream(
						RabbitMQUtils.readString(receivedBuffer).getBytes(StandardCharsets.UTF_8));
//				ResultSet received = ResultSetFactory.fromJSON(inReceived);
				
				// TODO check for results completness
				switch (queryType) {
					case 1:
						queryType1Sum += execTime;
						queryType1AvgExecTime = queryType1Sum / ++queryType1Count;
						break;
					case 2:	
						queryType2Sum += execTime;
						queryType2AvgExecTime = queryType2Sum / ++queryType2Count;
						break;
					case 3:	
						queryType3Sum += execTime;
						queryType3AvgExecTime = queryType3Sum / ++queryType3Count;
						break;
					case 4:	
						queryType4Sum += execTime;
						queryType4AvgExecTime = queryType4Sum / ++queryType4Count;
						break;
					case 5:	
						queryType5Sum += execTime;
						queryType5AvgExecTime = queryType5Sum / ++queryType5Count;
						break;
					case 6:	
						queryType6Sum += execTime;
						queryType6AvgExecTime = queryType6Sum / ++queryType6Count;
						break;
					case 7:	
						queryType7Sum += execTime;
						queryType7AvgExecTime = queryType7Sum / ++queryType7Count;
						break;
					case 8:	
						queryType8Sum += execTime;
						queryType8AvgExecTime = queryType8Sum / ++queryType8Count;
						break;
				}
				LOGGER.info("Query task of type: " + queryType + " executed in " + execTime + " ms and returned " + resultRowCount + "/" + expectedResultsNum + "results.");

				break;
		}
	}

	@Override
	protected Model summarizeEvaluation() throws Exception {
		LOGGER.info("Summarizing evaluation...");
		
		if (experimentUri == null) {
            Map<String, String> env = System.getenv();
            this.experimentUri = env.get(Constants.HOBBIT_EXPERIMENT_URI_KEY);
        }
		
		// compute the avgAppliedChangesPS
		avgAppliedChangesPS = totalAppliedChangesPS / changesPerVersion.size() - 1;
				
		// write the summarized results into a Jena model and send it to the benchmark controller.
		Model model = createDefaultModel();
		Resource experimentResource = model.getResource(experimentUri);
		model.add(experimentResource , RDF.type, HOBBIT.Experiment);
		
		Literal initialVersionIngestionSpeedLiteral = finalModel.createTypedLiteral(initialVersionIngestionSpeed, XSDDatatype.XSDdouble);
        finalModel.add(experimentResource, INITIAL_VERSION_INGESTION_SPEED, initialVersionIngestionSpeedLiteral);

		Literal avgAppliedChangesPSLiteral = finalModel.createTypedLiteral(avgAppliedChangesPS, XSDDatatype.XSDdouble);
        finalModel.add(experimentResource, AVG_APPLIED_CHANGES_PS, avgAppliedChangesPSLiteral);

        Literal storageCostLiteral = finalModel.createTypedLiteral(storageCost, XSDDatatype.XSDdouble);
        finalModel.add(experimentResource, STORAGE_COST, storageCostLiteral);

        Literal queryType1AvgExecTimeLiteral = finalModel.createTypedLiteral(queryType1AvgExecTime, XSDDatatype.XSDdouble);
        finalModel.add(experimentResource, QT_1_AVG_EXEC_TIME, queryType1AvgExecTimeLiteral);
        
        Literal queryType2AvgExecTimeLiteral = finalModel.createTypedLiteral(queryType2AvgExecTime, XSDDatatype.XSDdouble);
        finalModel.add(experimentResource, QT_2_AVG_EXEC_TIME, queryType2AvgExecTimeLiteral);
        
        Literal queryType3AvgExecTimeLiteral = finalModel.createTypedLiteral(queryType3AvgExecTime, XSDDatatype.XSDdouble);
        finalModel.add(experimentResource, QT_3_AVG_EXEC_TIME, queryType3AvgExecTimeLiteral);
        
        Literal queryType4AvgExecTimeLiteral = finalModel.createTypedLiteral(queryType4AvgExecTime, XSDDatatype.XSDdouble);
        finalModel.add(experimentResource, QT_4_AVG_EXEC_TIME, queryType4AvgExecTimeLiteral);
        
        Literal queryType5AvgExecTimeLiteral = finalModel.createTypedLiteral(queryType5AvgExecTime, XSDDatatype.XSDdouble);
        finalModel.add(experimentResource, QT_5_AVG_EXEC_TIME, queryType5AvgExecTimeLiteral);
        
        Literal queryType6AvgExecTimeLiteral = finalModel.createTypedLiteral(queryType6AvgExecTime, XSDDatatype.XSDdouble);
        finalModel.add(experimentResource, QT_6_AVG_EXEC_TIME, queryType6AvgExecTimeLiteral);
        
        Literal queryType7AvgExecTimeLiteral = finalModel.createTypedLiteral(queryType7AvgExecTime, XSDDatatype.XSDdouble);
        finalModel.add(experimentResource, QT_7_AVG_EXEC_TIME, queryType7AvgExecTimeLiteral);
        
        Literal queryType8AvgExecTimeLiteral = finalModel.createTypedLiteral(queryType8AvgExecTime, XSDDatatype.XSDdouble);
        finalModel.add(experimentResource, QT_8_AVG_EXEC_TIME, queryType8AvgExecTimeLiteral);
        
        return model;
	}
}
