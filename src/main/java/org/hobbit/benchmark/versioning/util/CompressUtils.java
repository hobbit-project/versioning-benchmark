package org.hobbit.benchmark.versioning.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompressUtils {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(CompressUtils.class);
	
	public static void compressGZIP(File input, File output) throws IOException {
		long start = System.currentTimeMillis();
        try (GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(output))) {
            try (FileInputStream in = new FileInputStream(input)) {
                byte[] buffer = new byte[1024];
                int len;
                while((len=in.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
            }
        }
    }
	
	public static void decompressGzip(File input, File output) throws IOException {
		long start = System.currentTimeMillis();
        try (GZIPInputStream in = new GZIPInputStream(new FileInputStream(input))) {
            try (FileOutputStream out = new FileOutputStream(output)) {
                byte[] buffer = new byte[1024];
                int len;
                while((len = in.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
            }
        }
    }
}
