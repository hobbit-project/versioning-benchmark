package org.hobbit.benchmark.versioning.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTPUtils {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(FTPUtils.class);

	public static void sendToFtp(String inputDir, String outputDir, String fileExtention, boolean compress) {
		FTPClient client = new FTPClient();
		FileInputStream fis = null;
		ByteArrayInputStream dataFilesIS = null;
		Properties ftpConfig = new Properties();
		
        try {
        	client.connect("hobbitdata.informatik.uni-leipzig.de");
        	LOGGER.info(client.sendNoOp() ? "Connection established to FTP server" : "Could not connect to FTP server");
        	client.enterLocalPassiveMode();
        	
    		ftpConfig.load(ClassLoader.getSystemResource("ftp.properties").openStream());
        	String username = ftpConfig.getProperty("username");
            String pwd = ftpConfig.getProperty("password");
        	LOGGER.info(client.login(username, pwd) ? "Logged in" : "Not connected to FTP.");
            
            // recursively create ftp folders if not exist
            boolean dirExists = true;
            String[] directories = outputDir.split("/");
            for (String dir : directories ) {
            	if (!dir.isEmpty() ) {
            		if (dirExists) {
            			dirExists = client.changeWorkingDirectory(dir);
            		}
	            	if (!dirExists) {
	                    if (!client.makeDirectory(dir)) {
	                    	throw new IOException("Unable to create remote directory '" + dir + "'.  error='" + client.getReplyString()+"'");
	                    }
	                    if (!client.changeWorkingDirectory(dir)) {
	                    	throw new IOException("Unable to change into newly created remote directory '" + dir + "'.  error='" + client.getReplyString()+"'");
	                    }
	            	}
            	}
            }

            File inputDirFile = new File(inputDir);
    		List<File> inputFiles = (List<File>) FileUtils.listFiles(inputDirFile, new String[] { fileExtention }, false);
    		StringBuilder dataFiles = new StringBuilder();
            for (File file : inputFiles) {
            	if (compress) {
            		File compressedFile = new File(file.getAbsolutePath() + ".gz");
            		CompressUtils.compressGZIP(file, compressedFile);
                	fis = new FileInputStream(compressedFile);
                	client.deleteFile(compressedFile.getAbsolutePath());
                	client.setFileType(FTP.BINARY_FILE_TYPE);
                	client.storeFile(compressedFile.getName(), fis);
                	dataFiles.append(compressedFile.getName() + "\n");
            	} else {
            		fis = new FileInputStream(file);
            		client.deleteFile(file.getAbsolutePath());
            		client.storeFile(file.getName(), fis);
                	dataFiles.append(file.getName() + "\n");
            	}
            }
            client.setFileType(FTP.ASCII_FILE_TYPE);
            dataFilesIS = new ByteArrayInputStream(dataFiles.toString().getBytes(StandardCharsets.UTF_8));
            client.storeFile("data_files.txt", dataFilesIS);
            client.logout();
        } catch (IOException e) {
			e.printStackTrace();
        } finally {
        	try {
                if (fis != null) {
                    fis.close();
                }
                client.disconnect();
            } catch (IOException e) {
    			e.printStackTrace();
            }
        	LOGGER.info("All files from " + inputDir + "successfully sent to FTP.");
        }
	}
}
