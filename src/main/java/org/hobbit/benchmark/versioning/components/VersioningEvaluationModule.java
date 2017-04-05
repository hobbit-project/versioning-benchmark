package org.hobbit.benchmark.versioning.components;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDF;
import org.hobbit.benchmark.versioning.systems.VirtuosoSystemAdapter;
import org.hobbit.core.components.AbstractEvaluationModule;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.vocab.HOBBIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersioningEvaluationModule extends AbstractEvaluationModule {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(VersioningEvaluationModule.class);

    private Model finalModel = ModelFactory.createDefaultModel();

	private double initialVersionIngestionSpeed = 0;
	private double avgNewVersionIngestionSpeed = 0;
	private double storageCost = 0;
	private double queryType1AvgExecTime;
	private double queryType2AvgExecTime;
	private double queryType3AvgExecTime;
	private double queryType4AvgExecTime;
	private double queryType5AvgExecTime;
	private double queryType6AvgExecTime;
	private double queryType7AvgExecTime;
	private double queryType8AvgExecTime;
	
	@Override
    public void init() throws Exception {
        // Always init the super class first!
        super.init();
    }

	@Override
	protected void evaluateResponse(byte[] expectedData, byte[] receivedData, long taskSentTimestamp,
			long responseReceivedTimestamp) throws Exception {
		
		ByteArrayInputStream exBis = new ByteArrayInputStream(expectedData);
		ByteBuffer receivedBuffer = ByteBuffer.wrap(receivedData);
		// get the query type
		String taskType = RabbitMQUtils.readString(receivedBuffer);
		
		switch (Integer.parseInt(taskType)) {
			case 1:
				// get the triples that successfully loaded by the system
				int loadedTriples = Integer.parseInt(RabbitMQUtils.readString(receivedBuffer));
				// get the time required to load them
				long loadingTime = Long.parseLong(RabbitMQUtils.readString(receivedBuffer));
				LOGGER.info("Ingestion task: " + loadedTriples + " triples loaded in " + loadingTime + " ms.");
				break;
			case 2:
				// get the disk space used
				long storageSpace = Long.parseLong(RabbitMQUtils.readString(receivedBuffer));
				LOGGER.info("Storage space task: " + storageSpace + " bytes.");
				break;
			case 3:	
				// get the query type
				int queryType = Integer.parseInt(RabbitMQUtils.readString(receivedBuffer));
				// get its execution time
				long execTime = Long.parseLong(RabbitMQUtils.readString(receivedBuffer));
				// get the results
				InputStream inReceived = new ByteArrayInputStream(
						RabbitMQUtils.readString(receivedBuffer).getBytes(StandardCharsets.UTF_8));
				ResultSet received = ResultSetFactory.fromJSON(inReceived);
				
//				switch (queryType) {
//					case 1:	
//						
//					case 2:	
//					case 3:	
//					case 4:	
//					case 5:	
//					case 6:	
//					case 7:	
//					case 8:	
//				}
				LOGGER.info("Query task of type: " + queryType + " executed in " + execTime + " ms and returned " + received.getRowNumber() + " results.");

				break;
		}
	}

	@Override
	protected Model summarizeEvaluation() throws Exception {
		// All tasks/responsens have been evaluated. Summarize the results,
		// write them into a Jena model and send it to the benchmark controller.
		Model model = createDefaultModel();
		Resource experimentResource = model.getResource(experimentUri);
		model.add(experimentResource , RDF.type, HOBBIT.Experiment);
		return model;
	}
}
