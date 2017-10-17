/**
 * 
 */
package org.hobbit.benchmark.versioning.systems;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.hobbit.benchmark.versioning.properties.RDFUtils;
import org.hobbit.benchmark.versioning.util.VirtuosoSystemAdapterConstants;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author papv
 * 
 */
public class VirtuosoSystemAdapter extends AbstractSystemAdapter {
			
	private static final Logger LOGGER = LoggerFactory.getLogger(VirtuosoSystemAdapter.class);
	
	private AtomicInteger totalReceived = new AtomicInteger(0);
	private AtomicInteger totalSent = new AtomicInteger(0);
	private Semaphore allVersionDataReceivedMutex = new Semaphore(0);

	// used to check if bulk loading phase has finished in  order to proceed with the querying phase
	private boolean dataLoadingFinished = false;
	private int loadingVersion = 0;
	
	// must match the "Generated data format" parameter given when starting the experiment
	private String generatedDataFormat = "n-triples";
	long initialDatasetsSize = 0;

	@Override
    public void init() throws Exception {
		LOGGER.info("Initializing virtuoso test system...");
        super.init();	
		LOGGER.info("Virtuoso initialized successfully .");
    }

	/* (non-Javadoc)
	 * @see org.hobbit.core.components.TaskReceivingComponent#receiveGeneratedData(byte[])
	 */
	public void receiveGeneratedData(byte[] data) {
		String dataPath = "/versioning/data/";
		String ontologiesPath = "/versioning/ontologies/";
		
		ByteBuffer dataBuffer = ByteBuffer.wrap(data);
		// read the graph uri in order to identify the version in which
		// received data will be loaded into.
		String graphUri = RabbitMQUtils.readString(dataBuffer);
		String receivedFilePath;
		
		if(graphUri.startsWith("http://datagen.ontology")) {
			receivedFilePath = ontologiesPath + graphUri.replaceFirst(".*/", "");
		} else if (graphUri.startsWith("http://datagen.version.0")) {
			receivedFilePath = dataPath + "v0/" + graphUri.replaceFirst(".*/", "");
		} else {
			String versionNum = graphUri.substring(25, graphUri.indexOf("generatedCreativeWorks") - 1);
			receivedFilePath = dataPath + "c" + versionNum + "/" + graphUri.replaceFirst(".*/", "");
		}
		// read the data contents
		byte[] dataContentBytes = new byte[dataBuffer.remaining()];
		dataBuffer.get(dataContentBytes, 0, dataBuffer.remaining());
		
		if (dataContentBytes.length != 0) {
			FileOutputStream fos = null;
			try {
				File outputFile = new File(receivedFilePath);
				fos = FileUtils.openOutputStream(outputFile, false);
				IOUtils.write(dataContentBytes, fos);
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
		
		if(totalReceived.incrementAndGet() == totalSent.get()) {
			allVersionDataReceivedMutex.release();
		}
	}

	/* (non-Javadoc)
	 * @see org.hobbit.core.components.TaskReceivingComponent#receiveGeneratedTask(java.lang.String, byte[])
	 */
	public void receiveGeneratedTask(String tId, byte[] data) {
		if(dataLoadingFinished) {
			LOGGER.info("Task " + tId + " received from task generator");
			
			// read the query
			String queryText = RabbitMQUtils.readString(data);

			Query query = QueryFactory.create(queryText);
			QueryExecution qexec = QueryExecutionFactory.sparqlService("http://localhost:8890/sparql", query);
			ResultSet rs = null;

			try {
				rs = qexec.execSelect();
			} catch (Exception e) {
				LOGGER.error("Task " + tId + " failed to execute.", e);
			}
			
			ByteArrayOutputStream queryResponseBos = new ByteArrayOutputStream();
			ResultSetFormatter.outputAsJSON(queryResponseBos, rs);
			byte[] results = queryResponseBos.toByteArray();
			LOGGER.info("Task " + tId + " executed successfully.");
			qexec.close();
			
			try {
				sendResultToEvalStorage(tId, results);
				LOGGER.info("Results sent to evaluation storage.");
			} catch (IOException e) {
				LOGGER.error("Exception while sending storage space cost to evaluation storage.", e);
			}
		} 
	}
	
	private void loadVersion(int versionNum) {
		LOGGER.info("Loading version " + versionNum + "...");
		try {
			String scriptFilePath = System.getProperty("user.dir") + File.separator + "load.sh";
			String[] command = {"/bin/bash", scriptFilePath, RDFUtils.getFileExtensionFromRdfFormat(generatedDataFormat), Integer.toString(versionNum) };
			Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ((line = in.readLine()) != null) {
				LOGGER.info(line);		
			}
			p.waitFor();
			LOGGER.info("Version " + versionNum + " loaded successfully.");
			in.close();
		} catch (IOException e) {
            LOGGER.error("Exception while executing script for loading data.", e);
		} catch (InterruptedException e) {
            LOGGER.error("Exception while executing script for loading data.", e);
		}
	}
	
	@Override
    public void receiveCommand(byte command, byte[] data) {
    	if (command == VirtuosoSystemAdapterConstants.BULK_LOAD_DATA_GEN_FINISHED) {
    		ByteBuffer buffer = ByteBuffer.wrap(data);
            int numberOfMessages = buffer.getInt();
            boolean lastLoadingPhase = buffer.get() != 0;
   			LOGGER.info("Received signal that all data of version " + loadingVersion + " successfully sent from all data generators (#" + numberOfMessages + ")");

			// if all data have been received before BULK_LOAD_DATA_GEN_FINISHED command received
   			// release before acquire, so it can immediately proceed to bulk loading
   			if(totalReceived.get() == totalSent.addAndGet(numberOfMessages)) {
				allVersionDataReceivedMutex.release();
			}
			
			LOGGER.info("Wait for receiving all data of version " + loadingVersion + ".");
			try {
				allVersionDataReceivedMutex.acquire();
			} catch (InterruptedException e) {
				LOGGER.error("Exception while waitting for all data of version " + loadingVersion + " to be recieved.", e);
			}
			
			LOGGER.info("All data of version " + loadingVersion + " received. Proceed to the loading of such version.");
			loadVersion(loadingVersion);
			
			LOGGER.info("Send signal to Benchmark Controller that all data of version " + loadingVersion + " successfully loaded.");
			try {
				sendToCmdQueue(VirtuosoSystemAdapterConstants.BULK_LOADING_DATA_FINISHED);
			} catch (IOException e) {
				LOGGER.error("Exception while sending signal that all data of version " + loadingVersion + " successfully loaded.", e);
			}
			loadingVersion++;
			dataLoadingFinished = lastLoadingPhase;
    	}
    	super.receiveCommand(command, data);
    }
	
	@Override
    public void close() throws IOException {
		LOGGER.info("Closing System Adapter...");
        // Always close the super class after yours!
        super.close();
        LOGGER.info("System Adapter closed successfully.");

    }
}