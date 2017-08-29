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
	
	private AtomicInteger availableMessages = new AtomicInteger(0);
	
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
        // get the disk space used by the system before any data is loaded to it
     	initialDatasetsSize = getDatasetSize();		
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
		int availableMsges = availableMessages.decrementAndGet();
		LOGGER.info("availableMessages=" + availableMsges + " .");
		if(availableMsges == 0) {
			allVersionDataReceivedMutex.release();
		}
	}

	/* (non-Javadoc)
	 * @see org.hobbit.core.components.TaskReceivingComponent#receiveGeneratedTask(java.lang.String, byte[])
	 */
	public void receiveGeneratedTask(String tId, byte[] data) {
		if(dataLoadingFinished) {
			LOGGER.info("Task " + tId + " received from task generator");
			boolean taskExecutedSuccesfully = true;
			
			ByteBuffer taskBuffer = ByteBuffer.wrap(data);
			// read the query type
			String taskType = RabbitMQUtils.readString(taskBuffer);
			// read the query
			String queryText = RabbitMQUtils.readString(taskBuffer);
			
			byte[][] resultsArray = null;
			// 1 stands for ingestion task
			// 2 for storage space task
			// 3 for SPARQL query task
			switch (Integer.parseInt(taskType)) {
				case 2:
					// get the storage space required for all versions to be stored in virtuoso
					long finalDatasetsSize = getDatasetSize();
					long storageSpaceCost = finalDatasetsSize - initialDatasetsSize;
					LOGGER.info("Total datasets size: "+ storageSpaceCost / 1000f + " Kbytes.");
	
					resultsArray = new byte[2][];
					resultsArray[0] = RabbitMQUtils.writeString(taskType);
					resultsArray[1] = RabbitMQUtils.writeString(Long.toString(storageSpaceCost));
					break;
				case 3:
					String queryType = queryText.substring(21, 22);
					LOGGER.info("queryType: " + queryType);
	
					Query query = QueryFactory.create(queryText);
					QueryExecution qexec = QueryExecutionFactory.sparqlService("http://localhost:8890/sparql", query);
					ResultSet results = null;
	
					try {
						results = qexec.execSelect();
					} catch (Exception e) {
						LOGGER.error("Task " + tId + " failed to execute.", e);
						taskExecutedSuccesfully = false;
					}
	
					resultsArray = new byte[4][];
					resultsArray[0] = RabbitMQUtils.writeString(taskType);
					resultsArray[1] = RabbitMQUtils.writeString(queryType);
					
					if(taskExecutedSuccesfully) {
						ByteArrayOutputStream queryResponseBos = new ByteArrayOutputStream();
						ResultSetFormatter.outputAsJSON(queryResponseBos, results);
						LOGGER.info("Results bytes size: " + queryResponseBos.size());

						int returnedResults = results.getRowNumber();
						
						resultsArray[2] = RabbitMQUtils.writeString(Integer.toString(returnedResults));
						resultsArray[3] = queryResponseBos.toByteArray();
						//debug
						LOGGER.info("system results_length: " + resultsArray[3].length);

//						resultsArray[3] = RabbitMQUtils.writeString("insteadOfOutpuStream");

						LOGGER.info("Task " + tId + " executed successfully and returned "+ returnedResults + " results.");
					} else {
						resultsArray[2] = RabbitMQUtils.writeString("-1");
						resultsArray[3] = RabbitMQUtils.writeString("-1");
						LOGGER.info("Task " + tId + " failed to executed. Error code (-1) set as result.");
					}
					qexec.close();
					break;
			}
			
			byte[] results = RabbitMQUtils.writeByteArrays(resultsArray);
			try {
				sendResultToEvalStorage(tId, results);
				LOGGER.info("Results sent to evaluation storage" + (taskExecutedSuccesfully ? "." : " for unsuccessful executed task."));
			} catch (IOException e) {
				LOGGER.error("Exception while sending storage space cost to evaluation storage.", e);
			}
		}
	}
	
	private long getDatasetSize() {
		checkpoint();
		File dbFile = new File("/usr/local/virtuoso-opensource/var/lib/virtuoso/db/virtuoso.db");
		return dbFile.length();
	}
	
	private void checkpoint() {
		String scriptFilePath = System.getProperty("user.dir") + File.separator + "checkpoint.sh";
		String[] command = {"/bin/bash", scriptFilePath };
		try {
			LOGGER.info("Executing checkpoint...");
			Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
			p.waitFor();
		} catch (IOException e) {
			LOGGER.error("Exception while executing checkpoint on virtuoso database.", e);
		} catch (InterruptedException e) {
			LOGGER.error("Exception while executing checkpoint on virtuoso database.", e);
		}
	}
	
	// returns the number of loaded triples to check if all version's triples loaded successfully.
	// have to return the total number of changes with respect to previous version in v2.0 of the benchmark 
	private String loadVersion(int versionNum) {
		LOGGER.info("Loading version " + versionNum + "...");
		String answer = null;
		try {
			String scriptFilePath = System.getProperty("user.dir") + File.separator + "load.sh";
			String[] command = {"/bin/bash", scriptFilePath, RDFUtils.getFileExtensionFromRdfFormat(generatedDataFormat), Integer.toString(versionNum) };
			Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ((line = in.readLine()) != null) {
				if(line.startsWith("triples")) {
					answer = line;
					continue;
				}
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
		return answer;
	}
	
	@Override
    public void receiveCommand(byte command, byte[] data) {
    	if (command == VirtuosoSystemAdapterConstants.BULK_LOAD_DATA_GEN_FINISHED) {
    		ByteBuffer dataBuffer = ByteBuffer.wrap(data);
			int numberOfMessagesSent = Integer.parseInt(RabbitMQUtils.readString(dataBuffer));
			boolean lastLoadingPhase = Boolean.getBoolean(RabbitMQUtils.readString(dataBuffer));
			LOGGER.info("Received signal that all data of version " + loadingVersion + " successfully sent from data generators (#" + numberOfMessagesSent + ")");
			availableMessages.addAndGet(numberOfMessagesSent);
			
			LOGGER.info("Wait for receiving all data of version " + loadingVersion + ".");
			try {
				allVersionDataReceivedMutex.acquire();
			} catch (InterruptedException e) {
				LOGGER.error("Exception while waitting for all data of version " + loadingVersion + " to be recieved.", e);
			}
			
			LOGGER.info("All data of version " + loadingVersion + " received. Proceed to the loading of such version.");
			loadVersion(loadingVersion++);
			
			LOGGER.info("Send signal to Benchmark Controller that all data of version " + loadingVersion + " successfully loaded.");
			try {
				sendToCmdQueue(VirtuosoSystemAdapterConstants.BULK_LOADING_DATA_FINISHED);
			} catch (IOException e) {
				LOGGER.error("Exception while sending signal that all data of version " + loadingVersion + " successfully loaded.", e);
			}
			dataLoadingFinished = !lastLoadingPhase;
    	}
    	super.receiveCommand(command, data);
    }
	
	@Override
    public void close() throws IOException {
		LOGGER.info("Closing System Adapter...");
        // Always close the super class after yours!
        super.close();
        //
		LOGGER.info("System Adapter closed successfully.");

    }
}