/**
 * 
 */
package org.hobbit.benchmark.versioning.systems;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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
public class R43plesSystemAdapter extends AbstractSystemAdapter {
		
	private static final Logger LOGGER = LoggerFactory.getLogger(R43plesSystemAdapter.class);
	private ArrayList<byte[][]> ingestionResultsArrays = new ArrayList<byte[][]>();
	private boolean dataGenFinished = false;
	private boolean dataLoadingFinished = false;
	
	// must match the "Generated data format" parameter given when starting the experiment
	private String generatedDataFormat = "n-triples";
	long initialDatasetsSize = 0;

	@Override
    public void init() throws Exception {
		LOGGER.info("Initializing virtuoso test system...");
        super.init();
        // get the disk space used by the system before any data is loaded to it
     	initialDatasetsSize = FileUtils.sizeOfDirectory(new File("/database/dataset"));
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
		String revision = null;
		if(receivedFilePath.startsWith("/versioning/ontologies") || receivedFilePath.startsWith("/versioning/data/v0")) {
			revision = "/toLoad/initial-version.nt";
		} else {
			String version = receivedFilePath.substring(18, receivedFilePath.lastIndexOf("/"));
			revision = "/toLoad/changeset-add-" + version + ".nt";
		}
		
		try {
			File outputFile = new File(revision);
			fos = FileUtils.openOutputStream(outputFile, true);
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
    	if (VirtuosoSystemAdapterConstants.BULK_LOAD_DATA_GEN_FINISHED == command) {
			LOGGER.info("Received signal from Data Generator that data generation finished.");
    		dataGenFinished = true;
    	} else if(VirtuosoSystemAdapterConstants.BULK_LOADING_DATA_FINISHED == command) {
			LOGGER.info("Received signal that all generated data loaded successfully.");
    		dataLoadingFinished = true;
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
