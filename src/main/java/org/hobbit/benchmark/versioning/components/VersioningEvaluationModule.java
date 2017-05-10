package org.hobbit.benchmark.versioning.components;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.hobbit.benchmark.versioning.IngestionStatistics;
import org.hobbit.benchmark.versioning.QueryTypeStatistics;
import org.hobbit.benchmark.versioning.properties.VersioningConstants;
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
    
    private IngestionStatistics is = new IngestionStatistics();
    private QueryTypeStatistics qts1 = new QueryTypeStatistics(1);
    private QueryTypeStatistics qts2 = new QueryTypeStatistics(2);
    private QueryTypeStatistics qts3 = new QueryTypeStatistics(3);
    private QueryTypeStatistics qts4 = new QueryTypeStatistics(4);
    private QueryTypeStatistics qts5 = new QueryTypeStatistics(5);
    private QueryTypeStatistics qts6 = new QueryTypeStatistics(6);
    private QueryTypeStatistics qts7 = new QueryTypeStatistics(7);
    private QueryTypeStatistics qts8 = new QueryTypeStatistics(8);
    
	private float storageCost = 0;

	@Override
    public void init() throws Exception {
		LOGGER.info("Initializing Evaluation Module...");
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
        
		LOGGER.info("Evaluation Module initialized successfully.");
    }
	
	/**
     * Initialize evaluation module parameters from environment variables
     * 
     * @param env		a map of all available environment variables
     * @param parameter	the property that we want to get
     */
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
		LOGGER.info("TASK TYPE: "+taskType);

		switch (Integer.parseInt(taskType)) {
			case 1:
				LOGGER.info("Evaluating response of an ingestion time task...");
				// get the loaded version
				int version = Integer.parseInt(RabbitMQUtils.readString(expectedBuffer));
				LOGGER.info("version: "+version);

				// get the triples that had to be loaded by the system
				int expectedLoadedTriples = Integer.parseInt(RabbitMQUtils.readString(expectedBuffer));
				LOGGER.info("expectedLoadedTriples: "+expectedLoadedTriples);

				// get the changes that successfully applied by the system
				int loadedTriples = Integer.parseInt(RabbitMQUtils.readString(receivedBuffer));
				LOGGER.info("loadedTriples: "+loadedTriples);

				// get the time, system requires to load the aformentioned triples
				long loadingTime = Long.parseLong(RabbitMQUtils.readString(receivedBuffer));
				LOGGER.info("loadingTime: "+loadingTime);

				if(loadedTriples != expectedLoadedTriples) {
					is.reportFailure();
					LOGGER.error(String.format("Total of %,d triples existed in the database, instead "
							+ "of %,d after loading of version %d", loadedTriples, expectedLoadedTriples, version));
				} else {
					is.reportSuccess(version, loadedTriples, loadingTime);
				}
				
				LOGGER.info("Ingestion task's response - Total triples after loading version " + version + ": " +
						loadedTriples + " of " + expectedLoadedTriples + ", loading time: " + loadingTime + " ms.");
				break;
			case 2:
				LOGGER.info("Evaluating response of storage space task...");
				// get the disk space used in KB
				storageCost = Long.parseLong(RabbitMQUtils.readString(receivedBuffer)) / 1000f;
				LOGGER.info("Response: " + storageCost + " KB.");
				break;
			case 3:	
				LOGGER.info("Evaluating response of query performance task...");

				// get the expected result's row number
				int expectedResultsNum = Integer.parseInt(RabbitMQUtils.readString(expectedBuffer));
				LOGGER.info("expectedResultsNum: "+expectedResultsNum);
				// get the expected results
				InputStream inExpected = new ByteArrayInputStream(
						RabbitMQUtils.readString(expectedBuffer).getBytes(StandardCharsets.UTF_8));
//				ResultSet received = ResultSetFactory.fromJSON(inExpected);
				
				// get the query type
				int queryType = Integer.parseInt(RabbitMQUtils.readString(receivedBuffer));
				LOGGER.info("queryType: "+queryType);
				// get its execution time
				long execTime = Long.parseLong(RabbitMQUtils.readString(receivedBuffer));
				LOGGER.info("execTime: "+execTime);
				// get the results row count
				int resultRowCount = Integer.parseInt(RabbitMQUtils.readString(receivedBuffer));
				LOGGER.info("resultRowCount: "+resultRowCount);
				// get the received results
				InputStream inReceived = new ByteArrayInputStream(
						RabbitMQUtils.readString(receivedBuffer).getBytes(StandardCharsets.UTF_8));
//				ResultSet received = ResultSetFactory.fromJSON(inReceived);
				
				boolean resultCompletness = resultRowCount == expectedResultsNum;
				boolean queryExecutedSuccesfully = resultRowCount != -1;
				boolean expAnswersComputedSuccesfuly = expectedResultsNum != -1;

				// TODO extend check for completness: do not only check the number of results
				switch (queryType) {
					case 1:
						if(resultCompletness && queryExecutedSuccesfully && expAnswersComputedSuccesfuly) {  
							qts1.reportSuccess(execTime); } 
						else { qts1.reportFailure(); }
						break;
					case 2:	
						if(resultCompletness && queryExecutedSuccesfully && expAnswersComputedSuccesfuly) {  
							qts2.reportSuccess(execTime); } 
						else { qts2.reportFailure(); }
						break;
					case 3:	
						if(resultCompletness && queryExecutedSuccesfully && expAnswersComputedSuccesfuly) {  
							qts3.reportSuccess(execTime); } 
						else { qts3.reportFailure(); }
						break;
					case 4:	
						if(resultCompletness && queryExecutedSuccesfully && expAnswersComputedSuccesfuly) {  
							qts4.reportSuccess(execTime); } 
						else { qts4.reportFailure(); }
						break;
					case 5:	
						if(resultCompletness && queryExecutedSuccesfully && expAnswersComputedSuccesfuly) {  
							qts5.reportSuccess(execTime); } 
						else { qts5.reportFailure(); }
						break;
					case 6:	
						if(resultCompletness && queryExecutedSuccesfully && expAnswersComputedSuccesfuly) {  
							qts6.reportSuccess(execTime); } 
						else { qts6.reportFailure(); }
						break;
					case 7:	
						if(resultCompletness && queryExecutedSuccesfully && expAnswersComputedSuccesfuly) {  
							qts7.reportSuccess(execTime); } 
						else { qts7.reportFailure(); }
						break;
					case 8:	
						if(resultCompletness && queryExecutedSuccesfully && expAnswersComputedSuccesfuly) {  
							qts8.reportSuccess(execTime); } 
						else { qts8.reportFailure(); }
						break;
				}
				LOGGER.info("Query task of type: " + queryType + " executed in " + execTime + " ms and returned " + resultRowCount + "/" + expectedResultsNum + " results.");

				break;
		}
	}

	@Override
	protected Model summarizeEvaluation() throws Exception {
		LOGGER.info("Summarizing evaluation...");
		
		LOGGER.info("Ingestion tasks errors: " + is.getFailuresCount());
		LOGGER.info("Query type 1 errors: " + qts1.getFailuresCount());
		LOGGER.info("Query type 2 errors: " + qts2.getFailuresCount());
		LOGGER.info("Query type 3 errors: " + qts3.getFailuresCount());
		LOGGER.info("Query type 4 errors: " + qts4.getFailuresCount());
		LOGGER.info("Query type 5 errors: " + qts5.getFailuresCount());
		LOGGER.info("Query type 6 errors: " + qts6.getFailuresCount());
		LOGGER.info("Query type 7 errors: " + qts7.getFailuresCount());
		LOGGER.info("Query type 8 errors: " + qts8.getFailuresCount());

		
		if (experimentUri == null) {
            Map<String, String> env = System.getenv();
            this.experimentUri = env.get(Constants.HOBBIT_EXPERIMENT_URI_KEY);
        }
		
		// write the summarized results into a Jena model and send it to the benchmark controller.
		Resource experimentResource = finalModel.getResource(experimentUri);
		finalModel.add(experimentResource , RDF.type, HOBBIT.Experiment);
		
		Literal initialVersionIngestionSpeedLiteral = finalModel.createTypedLiteral((is.getFailuresCount() == 0) ? is.getInitialVersionIngestionSpeed() : 0f, XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, INITIAL_VERSION_INGESTION_SPEED, initialVersionIngestionSpeedLiteral);
		LOGGER.info("INITIAL_VERSION_INGESTION_SPEED: " + 
				is.getInitialVersionIngestionSpeed() + "\n" + 
				initialVersionIngestionSpeedLiteral);

		Literal avgAppliedChangesPSLiteral = finalModel.createTypedLiteral(is.getAvgChangesPS(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, AVG_APPLIED_CHANGES_PS, avgAppliedChangesPSLiteral);
        LOGGER.info("AVG_APPLIED_CHANGES_PS: " + 
        		is.getAvgChangesPS() + "\n" + 
        		avgAppliedChangesPSLiteral);
        
        Literal storageCostLiteral = finalModel.createTypedLiteral(storageCost, XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, STORAGE_COST, storageCostLiteral);
        LOGGER.info("STORAGE_COST: " + 
        		storageCost + "\n" + 
        		storageCostLiteral);
        
        Literal queryType1AvgExecTimeLiteral = finalModel.createTypedLiteral(qts1.getAvgExecutionTimeMs(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QT_1_AVG_EXEC_TIME, queryType1AvgExecTimeLiteral);
        LOGGER.info("QT_1_AVG_EXEC_TIME: " + 
        		qts1.getAvgExecutionTimeMs() + "\n" + 
				queryType1AvgExecTimeLiteral);
        
        Literal queryType2AvgExecTimeLiteral = finalModel.createTypedLiteral(qts2.getAvgExecutionTimeMs(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QT_2_AVG_EXEC_TIME, queryType2AvgExecTimeLiteral);
        LOGGER.info("QT_2_AVG_EXEC_TIME: " + 
        		qts2.getAvgExecutionTimeMs() + "\n" + 
				queryType2AvgExecTimeLiteral);

        Literal queryType3AvgExecTimeLiteral = finalModel.createTypedLiteral(qts3.getAvgExecutionTimeMs(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QT_3_AVG_EXEC_TIME, queryType3AvgExecTimeLiteral);
        LOGGER.info("QT_3_AVG_EXEC_TIME: " + 
        		qts3.getAvgExecutionTimeMs() + "\n" + 
				queryType3AvgExecTimeLiteral);

        Literal queryType4AvgExecTimeLiteral = finalModel.createTypedLiteral(qts4.getAvgExecutionTimeMs(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QT_4_AVG_EXEC_TIME, queryType4AvgExecTimeLiteral);
        LOGGER.info("QT_4_AVG_EXEC_TIME: " + 
        		qts4.getAvgExecutionTimeMs() + "\n" + 
				queryType4AvgExecTimeLiteral);
        
        Literal queryType5AvgExecTimeLiteral = finalModel.createTypedLiteral(qts5.getAvgExecutionTimeMs(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QT_5_AVG_EXEC_TIME, queryType5AvgExecTimeLiteral);
        LOGGER.info("QT_5_AVG_EXEC_TIME: " + 
        		qts5.getAvgExecutionTimeMs() + "\n" + 
				queryType5AvgExecTimeLiteral);
        
        Literal queryType6AvgExecTimeLiteral = finalModel.createTypedLiteral(qts6.getAvgExecutionTimeMs(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QT_6_AVG_EXEC_TIME, queryType6AvgExecTimeLiteral);
        LOGGER.info("QT_6_AVG_EXEC_TIME: " + 
        		qts6.getAvgExecutionTimeMs() + "\n" + 
				queryType6AvgExecTimeLiteral);
        
        Literal queryType7AvgExecTimeLiteral = finalModel.createTypedLiteral(qts7.getAvgExecutionTimeMs(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QT_7_AVG_EXEC_TIME, queryType7AvgExecTimeLiteral);
        LOGGER.info("QT_7_AVG_EXEC_TIME: " + 
        		qts7.getAvgExecutionTimeMs() + "\n" + 
				queryType7AvgExecTimeLiteral);
        
        Literal queryType8AvgExecTimeLiteral = finalModel.createTypedLiteral(qts8.getAvgExecutionTimeMs(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QT_8_AVG_EXEC_TIME, queryType8AvgExecTimeLiteral);
        LOGGER.info("QT_8_AVG_EXEC_TIME: " + 
        		qts8.getAvgExecutionTimeMs() + "\n" + 
				queryType8AvgExecTimeLiteral);

        return finalModel;
	}
}