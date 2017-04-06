/**
 * 
 */
package org.hobbit.benchmark.versioning.systems;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.hobbit.benchmark.versioning.properties.RDFUtils;
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

	private String serializationFormat = "n-triples";
	
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
		
		ByteBuffer dataBuffer = ByteBuffer.wrap(data);
		// read the file path
		String receivedFilePath = RabbitMQUtils.readString(dataBuffer);
		// read the file contents
		byte[] fileContentBytes = RabbitMQUtils.readByteArray(dataBuffer);
		
		FileOutputStream fos = null;
		try {
			File outputFile = new File(receivedFilePath);
			fos = FileUtils.openOutputStream(outputFile, false);
			IOUtils.write(fileContentBytes, fos);
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

	/* (non-Javadoc)
	 * @see org.hobbit.core.components.TaskReceivingComponent#receiveGeneratedTask(java.lang.String, byte[])
	 */
	public void receiveGeneratedTask(String tId, byte[] data) {
		LOGGER.info("Task " + tId + " received from task generator");

		ByteBuffer taskBuffer = ByteBuffer.wrap(data);
		// read the query type
		String taskType = RabbitMQUtils.readString(taskBuffer);
		// read the query
		String queryText = RabbitMQUtils.readString(taskBuffer);
		
		// get the disk space used by the system before any data is loaded to it
		long initialDatasetsSize = getDatasetSize();		
		
		byte[][] resultsArray = null;
		// 1 stands for ingestion task
		// 2 for storage space task
		// 3 for SPARQL query task
		switch (Integer.parseInt(taskType)) {
			case 1:
				// get the version that will be loaded.
				int version = Integer.parseInt(queryText.substring(8, queryText.indexOf(",")));
				
				// load the version and measure the required time.
				long loadStart = System.currentTimeMillis();
				String loadedTriples = loadVersion(version);
				long loadEnd = System.currentTimeMillis();
				long loadingTime = loadEnd - loadStart;
				LOGGER.info("Version " + version + " loaded successfully in "+ loadingTime + " ms.");

				resultsArray = new byte[3][];
				resultsArray[0] = RabbitMQUtils.writeString(taskType);
				resultsArray[1] = RabbitMQUtils.writeString(loadedTriples);
				resultsArray[2] = RabbitMQUtils.writeLong(loadingTime);
				break;
			case 2:
				// get the storage space required for all versions to be stored in virtuoso
				long finalDatasetsSize = getDatasetSize();
				long storageSpaceCost = finalDatasetsSize - initialDatasetsSize;
				LOGGER.info("Total datasets size: "+ storageSpaceCost / 1000 + " Kbytes.");

				resultsArray = new byte[2][];
				resultsArray[0] = RabbitMQUtils.writeString(taskType);
				resultsArray[1] = RabbitMQUtils.writeLong(storageSpaceCost);
				break;
			case 3:
				String queryType = queryText.substring(21, 22);
				Query query = QueryFactory.create(queryText);
				QueryExecution qexec = QueryExecutionFactory.sparqlService("http://localhost:8890/sparql", query);
				long queryStart = System.currentTimeMillis();
				ResultSet results = qexec.execSelect();
				LOGGER.info("Results: " + results.getRowNumber());
				LOGGER.info("Exei: " + results.hasNext());
				long queryEnd = System.currentTimeMillis();
				long excecutionTime = queryEnd - queryStart;
				
				ByteArrayOutputStream queryResponseBos = new ByteArrayOutputStream();
				ResultSetFormatter.outputAsJSON(queryResponseBos, results);
				
				resultsArray = new byte[4][];
				resultsArray[0] = RabbitMQUtils.writeString(taskType);
				resultsArray[1] = RabbitMQUtils.writeString(queryType);
				resultsArray[2] = RabbitMQUtils.writeLong(excecutionTime);
				resultsArray[3] = queryResponseBos.toByteArray();
				break;
		}
		
		byte[] results = RabbitMQUtils.writeByteArrays(resultsArray);
		try {
			sendResultToEvalStorage(tId, results);
			LOGGER.info("Results sent to evaluation storage.");
		} catch (IOException e) {
			LOGGER.error("Exception while sending storage space cost to evaluation storage.", e);
		}
	}
	
	private long getDatasetSize() {
		File dbFile = new File("/usr/local/virtuoso-opensource/var/lib/virtuoso/db/virtuoso.db");
		return dbFile.length();
	}
	
	// returns the number of loaded triples to check if all version's triples loaded successfully.
	private String loadVersion(int versionNum) {
		LOGGER.info("Loading version " + versionNum + "...");
		String loadedTriples = null;
		try {
			String scriptFilePath = System.getProperty("user.dir") + File.separator + "virtuoso_system_load_triples.sh";
			String[] command = {"/bin/bash", scriptFilePath, RDFUtils.getFileExtensionFromRdfFormat(serializationFormat), Integer.toString(versionNum)};
			Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ((line = in.readLine()) != null) {
				if(line.matches("loaded triples ")) {
					loadedTriples = line.substring(15);
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
		return loadedTriples;
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
