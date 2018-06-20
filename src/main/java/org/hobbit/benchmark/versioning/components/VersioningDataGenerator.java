package org.hobbit.benchmark.versioning.components;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.query.ResultSetRewindable;
import org.hobbit.benchmark.versioning.Task;
import org.hobbit.benchmark.versioning.properties.RDFUtils;
import org.hobbit.benchmark.versioning.properties.VersioningConstants;
import org.hobbit.benchmark.versioning.util.FTPUtils;
import org.hobbit.benchmark.versioning.util.SystemAdapterConstants;
import org.hobbit.core.components.AbstractDataGenerator;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.ldbc.semanticpublishing.generators.data.DataGenerator;
import eu.ldbc.semanticpublishing.properties.Configuration;
import eu.ldbc.semanticpublishing.properties.Definitions;
import eu.ldbc.semanticpublishing.refdataset.DataManager;
import eu.ldbc.semanticpublishing.refdataset.model.Entity;
import eu.ldbc.semanticpublishing.resultanalyzers.LocationsAnalyzer;
import eu.ldbc.semanticpublishing.resultanalyzers.ReferenceDataAnalyzer;
import eu.ldbc.semanticpublishing.statistics.Statistics;
import eu.ldbc.semanticpublishing.substitutionparameters.SubstitutionParametersGenerator;
import eu.ldbc.semanticpublishing.substitutionparameters.SubstitutionQueryParametersManager;
import eu.ldbc.semanticpublishing.templates.MustacheTemplate;
import eu.ldbc.semanticpublishing.templates.VersioningMustacheTemplatesHolder;
import eu.ldbc.semanticpublishing.templates.versioning.*;
import eu.ldbc.semanticpublishing.util.AllocationsUtil;
import eu.ldbc.semanticpublishing.util.RandomUtil;

/**
 * Data Generator class for Versioning Benchmark.
 * 
 * @author Vassilis Papakonstantinou (papv@ics.forth.gr)
 *
 */
public class VersioningDataGenerator extends AbstractDataGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(VersioningDataGenerator.class);
		
	private int numberOfVersions;
	private int v0SizeInTriples;
	private int maxTriplesPerFile;
	private int dataGeneratorWorkers;
	private int seedYear;
	private int generorPeriodYears;
	private int subGeneratorSeed;
	private int subsParametersAmount;
	private int versionInsertionRatio;
	private int versionDeletionRatio;
	private String serializationFormat;
	private String sentDataForm;
	private String enabledQueryTypesParam;
	private String generatedDatasetPath = "/versioning/data";
	private String initialVersionDataPath = generatedDatasetPath + File.separator + "v0";
	private String ontologiesPath = "/versioning/ontologies";
	private String dbpediaPath = "/versioning/dbpedia";
	private String concValPath = "/versioning/dbpsb_concrete_values";

	private int[] triplesExpectedToBeAdded;
	private int[] triplesExpectedToBeDeleted;
	private int[] triplesExpectedToBeLoaded;
	private int[] cwsToBeLoaded;
	
	private Properties enabledQueryTypes = new Properties();
	
	private AtomicInteger numberOfmessages = new AtomicInteger(0);
	
	private Configuration configuration = new Configuration();
	private Definitions definitions = new Definitions();
	private RandomUtil randomGenerator = null;
	
	private VersioningMustacheTemplatesHolder versioningMustacheTemplatesHolder = new VersioningMustacheTemplatesHolder();
	private SubstitutionQueryParametersManager substitutionQueryParametersManager = new SubstitutionQueryParametersManager();

	private ArrayList<Task> tasks = new ArrayList<Task>();
	
	private int[] majorEvents;
	private int[] minorEvents;
	private int[] correlations;
	
	private int[] dbPediaVersionsDistribution;
	
	private Semaphore versionLoadedFromSystemMutex = new Semaphore(0);
		
	@Override
    public void init() throws Exception {
		LOGGER.info("Initializing Data Generator '" + getGeneratorId() + "'");
		super.init();
		
		String configurationFile = System.getProperty("user.dir") + File.separator + "test.properties";
		String definitionsFile = System.getProperty("user.dir") + File.separator + "definitions.properties";
		String dictionaryFile = System.getProperty("user.dir") + File.separator + "WordsDictionary.txt";
		
		// initialize the possible concrete values for DBSB query templates
		for(int year : new int[] { 2012, 2013, 2014, 2015, 2016 }) {		
			DataManager.strq1.put(year, (ArrayList<String>) FileUtils.readLines(new File(concValPath + File.separator + "strq1" + File.separator + year +".txt"), StandardCharsets.UTF_8));
			DataManager.strq2.put(year, (ArrayList<String>) FileUtils.readLines(new File(concValPath + File.separator + "strq2" + File.separator + year +".txt"), StandardCharsets.UTF_8));
			DataManager.strq3.put(year, (ArrayList<String>) FileUtils.readLines(new File(concValPath + File.separator + "strq3" + File.separator + year +".txt"), StandardCharsets.UTF_8));
			DataManager.strq4.put(year, (ArrayList<String>) FileUtils.readLines(new File(concValPath + File.separator + "strq4" + File.separator + year +".txt"), StandardCharsets.UTF_8));
			DataManager.strq5.put(year, (ArrayList<String>) FileUtils.readLines(new File(concValPath + File.separator + "strq5" + File.separator + year +".txt"), StandardCharsets.UTF_8));
			DataManager.strq6.put(year, (ArrayList<String>) FileUtils.readLines(new File(concValPath + File.separator + "strq6" + File.separator + year +".txt"), StandardCharsets.UTF_8));
		}
		configuration.loadFromFile(configurationFile); 
		definitions.loadFromFile(definitionsFile, configuration.getBoolean(Configuration.VERBOSE)); 
		
		maxTriplesPerFile = configuration.getInt(Configuration.GENERATED_TRIPLES_PER_FILE);
		dataGeneratorWorkers = configuration.getInt(Configuration.DATA_GENERATOR_WORKERS);
		subsParametersAmount = configuration.getInt(Configuration.QUERY_SUBSTITUTION_PARAMETERS);
		serializationFormat = configuration.getString(Configuration.GENERATE_CREATIVE_WORKS_FORMAT);
		seedYear = definitions.getInt(Definitions.YEAR_SEED);
		
		// Initialize data generation parameters through the environment variables given by user
		initFromEnv();
		triplesExpectedToBeAdded = new int[numberOfVersions];
		triplesExpectedToBeDeleted = new int[numberOfVersions];
		triplesExpectedToBeLoaded = new int[numberOfVersions];
		cwsToBeLoaded = new int[numberOfVersions];
		
		// load the enabled queries 
		Pattern pattern = Pattern.compile("QT([1-8])=([0|1])[^\\w]*");
		Matcher matcher = pattern.matcher(enabledQueryTypesParam);
		String enabledQueryTypesParamProp = "";
		while (matcher.find()) {
			enabledQueryTypesParamProp += "QT" + matcher.group(1) + "=" + matcher.group(2) + "\n";
		}
		enabledQueryTypes.load(new StringReader(enabledQueryTypesParamProp));
		
		// Given the above input, update configuration files that are necessary for data generation
		reInitializeSPBProperties();
		
		randomGenerator = new RandomUtil(dictionaryFile, subGeneratorSeed, seedYear, generorPeriodYears);
		definitions.initializeAllocations(randomGenerator.getRandom());
	
		// Set the nextId for Creative Works
		DataManager.creativeWorksNextId.set(configuration.getLong(Configuration.CREATIVE_WORK_NEXT_ID));
		
		// Pass the total number of versions that are going to be generated to SPB data generator
		DataManager.maxVersionNum = numberOfVersions - 1;

		// We need to generate the data, queries and gold standard here in order to be 
		// sure that all the above are already generated before the 1st task start to 
		// executed from the system.
		LOGGER.info("Populating reference data entities from the appropriated files...");
		this.populateRefDataEntitiesListsFromFiles();

		// Generate the data of the initial version.
		LOGGER.info("Generating Creative Works data files of the initial version...");
		long totalTriples = configuration.getLong(Configuration.DATASET_SIZE_TRIPLES);
		DataGenerator dataGenerator = new DataGenerator(randomGenerator, configuration, definitions, dataGeneratorWorkers, totalTriples, maxTriplesPerFile, initialVersionDataPath, serializationFormat);
		dataGenerator.produceData();
		cwsToBeLoaded[0] = v0SizeInTriples;
		triplesExpectedToBeAdded[0] = dataGenerator.getTriplesGeneratedSoFar().intValue();
		triplesExpectedToBeDeleted[0] = 0;

		// Generate the change sets. Additions and deletions are supported.
		// TODO: support changes
		int preVersionDeletedCWs = 0;
		long changeSetStart = System.currentTimeMillis();
		for(int i = 1; i < numberOfVersions; i++) {
			String destinationPath = generatedDatasetPath + File.separator + "c" + i;
			File theDir = new File(destinationPath);
			theDir.mkdir();
			
			int triplesToBeAdded = Math.round(versionInsertionRatio / 100f * cwsToBeLoaded[i-1]);
			int triplesToBeDeleted = Math.round(versionDeletionRatio / 100f * cwsToBeLoaded[i-1]);
			cwsToBeLoaded[i] = cwsToBeLoaded[i-1] + triplesToBeAdded - triplesToBeDeleted;
			LOGGER.info("Generating version " + i + " changeset. Target: " + "[+" + String.format(Locale.US, "%,d", triplesToBeAdded).replace(',', '.') + " , -" + String.format(Locale.US, "%,d", triplesToBeDeleted).replace(',', '.') + "]");
						
			// produce the delete set
			LOGGER.info("Generating version " + i + " delete-set.");
			long deleteSetStart = System.currentTimeMillis();
			int currVersionDeletedCreativeWorks = 0;
			int currVersionDeletedTriples = 0;
			int totalRandomTriplesSoFar =  DataManager.randomCreativeWorkTriples.intValue();
			ArrayList<String> cwToBeDeleted = new ArrayList<String>();
			
			// if the number of triples that have to be deleted is larger than the already existing 
			// random-model ones, use all the random as a threshold
			List<Long> randomCreativeWorkIds = new ArrayList<Long>(DataManager.randomCreativeWorkIdsList.keySet());
			if(triplesToBeDeleted > totalRandomTriplesSoFar) {
				LOGGER.info("Target of " + String.format(Locale.US, "%,d", triplesToBeDeleted).replace(',', '.')
						+ " triples exceedes the already (random-model) existing ones (" 
						+ String.format(Locale.US, "%,d", totalRandomTriplesSoFar).replace(',', '.') + "). "
						+ "Will choose all of them.");
				// take all the random
				for (long creativeWorkId : randomCreativeWorkIds) {
					cwToBeDeleted.add("http://www.bbc.co.uk/things/" + getGeneratorId() + "-" + creativeWorkId + "#id");
				}
				DataManager.randomCreativeWorkIdsList.clear();
				DataManager.randomCreativeWorkTriples.set(0);
				currVersionDeletedTriples = totalRandomTriplesSoFar;
		
				// write down the creative work uris that are going to be deleted
				FileUtils.writeLines(new File("/versioning/creativeWorksToBeDeleted.txt") , cwToBeDeleted, false);

				// extract all triples that have to be deleted using multiple threads
				parallelyExtract(i, destinationPath);
				currVersionDeletedCreativeWorks += cwToBeDeleted.size();
			} else {
				while (currVersionDeletedTriples < triplesToBeDeleted) {
					int creativeWorkToBeDeletedIdx = randomGenerator.nextInt(randomCreativeWorkIds.size());
					long creativeWorkToBeDeleted = randomCreativeWorkIds.get(creativeWorkToBeDeletedIdx);
					currVersionDeletedTriples += DataManager.randomCreativeWorkIdsList.get(creativeWorkToBeDeleted);
					randomCreativeWorkIds.remove(creativeWorkToBeDeletedIdx);
					DataManager.randomCreativeWorkIdsList.remove(creativeWorkToBeDeleted);
					cwToBeDeleted.add("http://www.bbc.co.uk/things/" + getGeneratorId() + "-" + creativeWorkToBeDeleted + "#id");
				}
				// write down the creative work uris that are going to be deleted
				// in order to use it in grep -F -f
				FileUtils.writeLines(new File("/versioning/creativeWorksToBeDeleted.txt") , cwToBeDeleted, false);

				// extract all triples that have to be deleted using multiple threads
				parallelyExtract(i, destinationPath);
				DataManager.randomCreativeWorkTriples.addAndGet(-currVersionDeletedTriples);
				currVersionDeletedCreativeWorks += cwToBeDeleted.size();
			}
			preVersionDeletedCWs = currVersionDeletedCreativeWorks;
			triplesExpectedToBeDeleted[i] = currVersionDeletedTriples;
			long deleteSetEnd = System.currentTimeMillis();
			LOGGER.info("Deleteset of total " + String.format(Locale.US, "%,d", preVersionDeletedCWs).replace(',', '.') + " Creative Works generated successfully. Triples: " + String.format(Locale.US, "%,d", currVersionDeletedTriples).replace(',', '.') + " . Target: " + String.format(Locale.US, "%,d", triplesToBeDeleted).replace(',', '.') + " triples. Time: " + (deleteSetEnd - deleteSetStart) + " ms.");

			// produce the add set
			LOGGER.info("Generating version " + i + " add-set.");
			dataGenerator.produceAdded(destinationPath, triplesToBeAdded);
			triplesExpectedToBeAdded[i] = dataGenerator.getTriplesGeneratedSoFar().intValue();

		}
		long changeSetEnd = System.currentTimeMillis();
		LOGGER.info("All changesets generated successfully. Time: " + (changeSetEnd - changeSetStart) + " ms.");

		// Evenly distribute the 5 dbpedia versions to the total number of versions that were generated
		distributeDBpediaVersions();

		LOGGER.info("Generating tasks...");
		// 3) Generate SPARQL query tasks
		// generate benchmark tasks substitution parameters
		String queriesPath = System.getProperty("user.dir") + File.separator + "query_templates";
		versioningMustacheTemplatesHolder.loadFrom(queriesPath);		
		generateQuerySubstitutionParameters();
		// initialize substitution parameters
		String substitutionParametersPath = System.getProperty("user.dir") + File.separator + "substitution_parameters";
		LOGGER.info("Initializing parameters for SPARQL query tasks...");
		substitutionQueryParametersManager.initVersioningSubstitutionParameters(substitutionParametersPath, false, false);
		LOGGER.info("Query parameters initialized successfully.");
		// build mustache templates to create queries
		LOGGER.info("Building SPRQL tasks...");
		buildSPRQLQueries();
		LOGGER.info("All SPRQL tasks built successfully.");	

		LOGGER.info("Loading generating data, in order to compute gold standard...");
		// load generated creative works to virtuoso, in order to compute the gold standard
		loadFirstNVersions(numberOfVersions);
			
		// compute expected answers for all tasks
		LOGGER.info("Computing expected answers for generated SPARQL tasks...");
		computeExpectedAnswers();
		LOGGER.info("Expected answers have computed successfully for all generated SPRQL tasks.");	
        LOGGER.info("Data Generator initialized successfully.");
	}
	
	public void parallelyExtract(int currVersion, String destinationPath) {
		ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		for(int j = 0; j < currVersion; j++) {
			String sourcePath = generatedDatasetPath + File.separator + (j == 0 ? "v" : "c") + j + File.separator;
			File sourcePathFile = new File(sourcePath);
	    	List<File> previousVersionAddedFiles = (List<File>) FileUtils.listFiles(sourcePathFile, new RegexFileFilter("generatedCreativeWorks-[0-9]+-[0-9]+.added.nt"),  null);
			for (File f : previousVersionAddedFiles) {
				executor.execute(new ExtractDeleted(f, "/versioning/creativeWorksToBeDeleted.txt", destinationPath));
			}
		}
		executor.shutdown();
		try {
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS); // no timeout
		} catch (InterruptedException e) {
			LOGGER.error("Exception caught while awaiting termination...", e);
		}
	}
	
	// class for implementing extraction of triples that have to be deleted concurrently
	public static class ExtractDeleted implements Runnable {
		private File file;
		private String cwTBD;
		private String destinationPath;
 
		ExtractDeleted(File file, String cwTBD, String destinationPath) {
			this.file = file;
			this.cwTBD = cwTBD;
			this.destinationPath = destinationPath;
		}
 
		public void run() {
			try {
				extractDeleted(file.getAbsolutePath(), cwTBD, destinationPath);
			} catch (Exception e) {
				LOGGER.error("Exception caught during the extraction of deleted triples from " + file, e);
			}
		}
	}
	
	public void initFromEnv() {
		LOGGER.info("Getting data generator configuration parameters...");
		
		Map<String, String> env = System.getenv();
		// Assume that in v0Size the 40362 triples of DBpedia initial dataset 
		// plus the 8135 ontologies triples are included
		v0SizeInTriples = (Integer) getFromEnv(env, VersioningConstants.V0_SIZE_IN_TRIPLES, 0) - 48497 ;
		numberOfVersions = (Integer) getFromEnv(env, VersioningConstants.NUMBER_OF_VERSIONS, 0);
		subGeneratorSeed = (Integer) getFromEnv(env, VersioningConstants.DATA_GENERATOR_SEED, 0) + getGeneratorId();
		versionInsertionRatio = (Integer) getFromEnv(env, VersioningConstants.VERSION_INSERTION_RATIO, 0);
		versionDeletionRatio = (Integer) getFromEnv(env, VersioningConstants.VERSION_DELETION_RATIO, 0);
		sentDataForm = (String) getFromEnv(env, VersioningConstants.SENT_DATA_FORM, "");
		enabledQueryTypesParam = (String) getFromEnv(env, VersioningConstants.ENABLED_QUERY_TYPES, "");
	}	
	
	/*
	 *  Equally distribute the total number of dbpedia versions (5) to the total number of 
	 *  versions that have to be produced. '1' values appeared in the dbPediaVersionsDistribution 
	 *  array, denote that in such a position (representing the version) a DBpedia version will 
	 *  also included to the generated data.
	 *  Such method have to be called after the dataGenerator.produceData(), as it requires
	 *  each versions underline path to already been generated and before computation of expected 
	 *  answers.
	 */
	private void distributeDBpediaVersions() {		
		int[] triplesToBeAdded = { 
				VersioningConstants.DBPEDIA_ADDED_TRIPLES_V0, 
				VersioningConstants.DBPEDIA_ADDED_TRIPLES_V1, 
				VersioningConstants.DBPEDIA_ADDED_TRIPLES_V2, 
				VersioningConstants.DBPEDIA_ADDED_TRIPLES_V3, 
				VersioningConstants.DBPEDIA_ADDED_TRIPLES_V4 }; 
		int[] triplesToBeDeleted = { 
				VersioningConstants.DBPEDIA_DELETED_TRIPLES_V0, 
				VersioningConstants.DBPEDIA_DELETED_TRIPLES_V1, 
				VersioningConstants.DBPEDIA_DELETED_TRIPLES_V2, 
				VersioningConstants.DBPEDIA_DELETED_TRIPLES_V3, 
				VersioningConstants.DBPEDIA_DELETED_TRIPLES_V4 };
		
		// list the 5 dbpedia files
		String dbpediaFinalPath = dbpediaPath + File.separator + "final";
    	File finalDbpediaPathFile = new File(dbpediaFinalPath);
    	List<File> finalDbpediaFiles = (List<File>) FileUtils.listFiles(finalDbpediaPathFile, new String[] { "nt" }, false);
		Collections.sort(finalDbpediaFiles);
		
		// list the changesets for dbpedia files
		String changesetsPath = dbpediaPath + File.separator + "changesets";
    	File changesetsDbpediaPathFile = new File(changesetsPath);
		List<File> addedDataFiles = (List<File>) FileUtils.listFiles(changesetsDbpediaPathFile, new String[] { "added.nt" }, false);
		Collections.sort(addedDataFiles);
		List<File> deletedDataFiles = (List<File>) FileUtils.listFiles(changesetsDbpediaPathFile, new String[] { "deleted.nt" }, false);
		Collections.sort(deletedDataFiles);
		
		// if the number of versions that have to be produced is larger than the total 5 of dbpedia
		// determine in which versions the dbpedia ones will be assigned
		dbPediaVersionsDistribution = new int[numberOfVersions];
		if(numberOfVersions > 5) {
			LOGGER.info("Distributing the 5 DBpedia versions to the total " + numberOfVersions + " produced...");
			Arrays.fill(dbPediaVersionsDistribution, 0);
			for(int dbpediaVersion = 0; dbpediaVersion < VersioningConstants.DBPEDIA_VERSIONS; dbpediaVersion++) {
				int versionIndex = Math.round((numberOfVersions - 1) * (dbpediaVersion / 4f));
				dbPediaVersionsDistribution[versionIndex] = 1;
				triplesExpectedToBeAdded[versionIndex] += triplesToBeAdded[dbpediaVersion];
				triplesExpectedToBeDeleted[versionIndex] += triplesToBeDeleted[dbpediaVersion];
			}
		} else {
			LOGGER.info("Assigning the first " + numberOfVersions + " DBpedia versions to the total " + numberOfVersions + " produced...");
			Arrays.fill(dbPediaVersionsDistribution, 1);
			for(int dbpediaVersion = 0; dbpediaVersion < numberOfVersions; dbpediaVersion++) {
				triplesExpectedToBeAdded[dbpediaVersion] += triplesToBeAdded[dbpediaVersion];
				triplesExpectedToBeDeleted[dbpediaVersion] += triplesToBeDeleted[dbpediaVersion];
			}
		}	
		
		LOGGER.info("Distribution: " + Arrays.toString(dbPediaVersionsDistribution));
		
		// copy the dbpedia file to the appropriate version dir, (when dbPediaVersionsDistribution[i] = 1)
		for (int i = 0, dbpediaIndex = 0; i < dbPediaVersionsDistribution.length; i++) {
			if (dbPediaVersionsDistribution[i] == 1) {
				try {
					String destinationParent = generatedDatasetPath + File.separator + (i == 0 ? "v" : "c") + i + File.separator;							

					// copy the final dbpedia file that will be used from the datagenerator
					File finalFrom = finalDbpediaFiles.get(dbpediaIndex);
					File finalTo = new File(destinationParent + "dbpedia_final" + File.separator + finalFrom.getName());
					FileUtils.copyFile(finalFrom, finalTo);					
					
					// copy the addset that will be sent to the system
					File addedFrom = addedDataFiles.get(dbpediaIndex);
					File addedTo = new File(destinationParent + addedFrom.getName());
					FileUtils.copyFile(addedFrom, addedTo);
					
					if(i > 0) {
						// copy the deletset that will be sent to the system
						// dbpediaIndex-1 because for version 0 we do not have deleted triples
						File deletedFrom = deletedDataFiles.get(dbpediaIndex - 1);
						File deletedTo = new File(destinationParent + deletedFrom.getName());
						FileUtils.copyFile(deletedFrom, deletedTo);
					}
					dbpediaIndex++;
				} catch(IOException e) {
					LOGGER.error("Exception caught during the copy of dbpedia files to the appropriate version dir", e);
				}
			}
		}
	}

	public String compileMustacheTemplate(int queryType, int queryIndex, int subsParameterIndex) {
		String[] querySubstParameters = substitutionQueryParametersManager.getSubstitutionParametersFor(SubstitutionQueryParametersManager.QueryType.VERSIONING, queryIndex).get(subsParameterIndex);
		String compiledQuery = null;
		switch (queryType) {
			case 1 :
				MustacheTemplate versioningQuery1_1 = new VersioningQuery1_1Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
				compiledQuery = versioningQuery1_1.compileMustacheTemplate();
				break;
			case 2 :
				if(queryIndex == 1) {
					MustacheTemplate versioningQuery2_1 = new VersioningQuery2_1Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery2_1.compileMustacheTemplate();
					break;
				} else if (queryIndex == 2) {
					MustacheTemplate versioningQuery2_2 = new VersioningQuery2_2Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery2_2.compileMustacheTemplate();
					break;
				} else if (queryIndex == 3) {
					MustacheTemplate versioningQuery2_3 = new VersioningQuery2_3Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery2_3.compileMustacheTemplate();
					break;
				} else if (queryIndex == 4) {
					MustacheTemplate versioningQuery2_4 = new VersioningQuery2_4Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery2_4.compileMustacheTemplate();
					break;
				} else if (queryIndex == 5) {
					MustacheTemplate versioningQuery2_5 = new VersioningQuery2_5Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery2_5.compileMustacheTemplate();
					break;
				} else if (queryIndex == 6) {
					MustacheTemplate versioningQuery2_6 = new VersioningQuery2_6Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery2_6.compileMustacheTemplate();
					break;
				}
			case 3 : 
				MustacheTemplate versioningQuery3_1 = new VersioningQuery3_1Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
				compiledQuery = versioningQuery3_1.compileMustacheTemplate();
				break;
			case 4 :
				if(queryIndex == 8) {
					MustacheTemplate versioningQuery4_1 = new VersioningQuery4_1Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery4_1.compileMustacheTemplate();
					break;
				} else if (queryIndex == 9) {
					MustacheTemplate versioningQuery4_2 = new VersioningQuery4_2Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery4_2.compileMustacheTemplate();
					break;
				} else if (queryIndex == 10) {
					MustacheTemplate versioningQuery4_3 = new VersioningQuery4_3Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery4_3.compileMustacheTemplate();
					break;
				} else if (queryIndex == 11) {
					MustacheTemplate versioningQuery4_4 = new VersioningQuery4_4Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery4_4.compileMustacheTemplate();
					break;
				} else if (queryIndex == 12) {
					MustacheTemplate versioningQuery4_5 = new VersioningQuery4_5Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery4_5.compileMustacheTemplate();
					break;
				} else if (queryIndex == 13) {
					MustacheTemplate versioningQuery4_6 = new VersioningQuery4_6Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery4_6.compileMustacheTemplate();
					break;
				}
			case 5 :
				MustacheTemplate versioningQuery5_1 = new VersioningQuery5_1Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
				compiledQuery = versioningQuery5_1.compileMustacheTemplate();
				break;
			case 6 : 
				MustacheTemplate versioningQuery6_1 = new VersioningQuery6_1Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
				compiledQuery = versioningQuery6_1.compileMustacheTemplate();
				break;
			case 7 :
				MustacheTemplate versioningQuery7_1 = new VersioningQuery7_1Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
				compiledQuery = versioningQuery7_1.compileMustacheTemplate();
				break;			
			case 8 :
				if(queryIndex == 17) {
					MustacheTemplate versioningQuery8_1 = new VersioningQuery8_1Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery8_1.compileMustacheTemplate();
					break;
				} else if (queryIndex == 18) {
					MustacheTemplate versioningQuery8_2 = new VersioningQuery8_2Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery8_2.compileMustacheTemplate();
					break;
				} else if (queryIndex == 19) {
					MustacheTemplate versioningQuery8_3 = new VersioningQuery8_3Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery8_3.compileMustacheTemplate();
					break;
				} else if (queryIndex == 20) {
					MustacheTemplate versioningQuery8_4 = new VersioningQuery8_4Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery8_4.compileMustacheTemplate();
					break;
				} else if (queryIndex == 21) {
					MustacheTemplate versioningQuery8_5 = new VersioningQuery8_5Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery8_5.compileMustacheTemplate();
					break;
				} else if (queryIndex == 22) {
					MustacheTemplate versioningQuery8_6 = new VersioningQuery8_6Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters, null);
					compiledQuery = versioningQuery8_6.compileMustacheTemplate();
					break;
				} 
		}
		return compiledQuery;
	}
	
	private int getVersionSize(int versionNum) {
		String sparqlQueryString = ""
				+ "SELECT (COUNT(*) AS ?cnt) "
				+ "FROM <http://graph.version." + versionNum + "> "
				+ "WHERE { ?s ?p ?o }";
		
		try (QueryExecution qexec = QueryExecutionFactory.sparqlService("http://localhost:8890/sparql", sparqlQueryString)) {
			ResultSet results = ResultSetFactory.makeRewindable(qexec.execSelect());
			if(results.hasNext()) {
				return results.next().getLiteral("cnt").getInt();
			}
		} catch (Exception e) {
			LOGGER.error("Exception caught during the computation of version " + versionNum + " triples number.", e);
		}
		return 0;
	}
		
	public void computeExpectedAnswers() {	
		// compute the number of triples that expected to be loaded by the system.
		// so the evaluation module can compute the ingestion and average changes speeds
		for (int version = 0; version < numberOfVersions; version++) {
			triplesExpectedToBeLoaded[version] = getVersionSize(version);
		}
		
		for (Task task : tasks) {			
			ResultSetRewindable results = null;

			// execute the query on top of virtuoso to compute the expected answers
			long queryStart = System.currentTimeMillis();
			try (QueryExecution qexec = QueryExecutionFactory.sparqlService("http://localhost:8890/sparql", task.getQuery())) {
				results = ResultSetFactory.makeRewindable(qexec.execSelect());
			} catch (Exception e) {
				LOGGER.error("Exception caught during the computation of task " + task.getTaskId() + " expected answers.", e);
			}
			long queryEnd = System.currentTimeMillis();			

			// update the task by setting its expected results
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			ResultSetFormatter.outputAsJSON(outputStream, results);
			//debug
			LOGGER.info("Expected answers for task " + task.getTaskId() + " computed"
					+ ". Type: " + task.getQueryType() + "." + task.getQuerySubType() 
					+ ", ResultsNum: " + results.size() 
					+ ", Time: " + (queryEnd - queryStart) + " ms.");			

			task.setExpectedAnswers(outputStream.toByteArray());
			tasks.set(Integer.parseInt(task.getTaskId()), task);
		}	
	}
	
	public void buildSPRQLQueries() {
		int taskId = 0;
		int queryIndex = 0;
		int querySubstParamCount = 0;
		String queriesPath = System.getProperty("user.dir") + File.separator + "queries" + File.separator;
		String queryString;
		
		// QT1
		if(enabledQueryTypes.getProperty("QT1", "0").equals("1")) {
			queryString = compileMustacheTemplate(1, queryIndex, 0);
			tasks.add(new Task(1, 1, Integer.toString(taskId++), queryString, null));
			try {
				FileUtils.writeStringToFile(new File(queriesPath + "versioningQuery1.1.1.sparql"), queryString);
			} catch (IOException e) {
				LOGGER.error("Exception caught during saving of generated task : ", e);
			}
		}
		queryIndex++;
		
		// QT2
		if(enabledQueryTypes.getProperty("QT2", "0").equals("1")) {
			querySubstParamCount = 5;
			for (int querySubType = 1; querySubType <= Statistics.VERSIONING_SUB_QUERIES_COUNT; querySubType++) {
				for(int querySubstParam = 1; querySubstParam <= querySubstParamCount; querySubstParam++) {
					queryString = compileMustacheTemplate(2, queryIndex, querySubstParam);
					tasks.add(new Task(2, querySubType, Integer.toString(taskId++), queryString, null));
					try {
						FileUtils.writeStringToFile(new File(queriesPath + "versioningQuery2." + querySubType + "." + querySubstParam + ".sparql"), queryString);
					} catch (IOException e) {
						LOGGER.error("Exception caught during saving of generated task : ", e);
					}
				}
				queryIndex++;
			}
		} else {
			queryIndex += Statistics.VERSIONING_SUB_QUERIES_COUNT;
		}
		
		// if there is only one version return
		if(numberOfVersions == 1) 
			return;

		// QT3
		// if the total number of versions is lower than 1 there are no historical versions
		if(enabledQueryTypes.getProperty("QT3", "0").equals("1")) {
			querySubstParamCount = 3;
			for(int querySubstParam = 1; querySubstParam <= querySubstParamCount && querySubstParam < numberOfVersions ; querySubstParam++) {
				queryString = compileMustacheTemplate(3, queryIndex, querySubstParam);
				tasks.add(new Task(3, 1, Integer.toString(taskId++), queryString, null));
				try {
					FileUtils.writeStringToFile(new File(queriesPath + "versioningQuery3.1." + querySubstParam + ".sparql"), queryString);
				} catch (IOException e) {
					LOGGER.error("Exception caught during saving of generated task : ", e);
				}
			}
		} 
		queryIndex++;
		
		// QT4
		// if the total number of versions is lower than 1 there are no historical versions
		if(enabledQueryTypes.getProperty("QT4", "0").equals("1")) {
			querySubstParamCount = 3;
			for (int querySubType = 1; querySubType <= Statistics.VERSIONING_SUB_QUERIES_COUNT; querySubType++) {
				// if there are less than four versions take all the historical ones
				for(int querySubstParam = 1; querySubstParam <= querySubstParamCount && querySubstParam < numberOfVersions; querySubstParam++) {
					queryString = compileMustacheTemplate(4, queryIndex, querySubstParam);
					tasks.add(new Task(4, querySubType, Integer.toString(taskId++), queryString, null));
					try {
						FileUtils.writeStringToFile(new File(queriesPath + "versioningQuery4." + querySubType + "." + querySubstParam + ".sparql"), queryString);
					} catch (IOException e) {
						LOGGER.error("Exception caught during saving of generated task : ", e);
					}
				}
				queryIndex++;
			}
		} else {
			queryIndex += Statistics.VERSIONING_SUB_QUERIES_COUNT;
		}
		
		// if the total number of versions is lower than 5 read deltas for all the historical ones
		// QT5 and QT6
		if(numberOfVersions < 5) {
			querySubstParamCount = numberOfVersions - 1;
		} else {
			querySubstParamCount = 4;
		}
		// QT5
		if(enabledQueryTypes.getProperty("QT5", "0").equals("1")) {
			for(int querySubstParam = 1; querySubstParam <= querySubstParamCount; querySubstParam++) {
				queryString = compileMustacheTemplate(5, queryIndex, querySubstParam);
				tasks.add(new Task(5, 1, Integer.toString(taskId++), queryString, null));
				try {
					FileUtils.writeStringToFile(new File(queriesPath + "versioningQuery5.1." + querySubstParam + ".sparql"), queryString);
				} catch (IOException e) {
					LOGGER.error("Exception caught during saving of generated task : ", e);
				}
			}
		}
		queryIndex++;
		
		// QT6
		// same querySubstParamCount as QT5
		if(enabledQueryTypes.getProperty("QT6", "0").equals("1")) {
			for(int querySubstParam = 1; querySubstParam <= querySubstParamCount; querySubstParam++) {
				queryString = compileMustacheTemplate(6, queryIndex, querySubstParam);
				tasks.add(new Task(6, 1, Integer.toString(taskId++), queryString, null));
				try {
					FileUtils.writeStringToFile(new File(queriesPath + "versioningQuery6.1." + querySubstParam + ".sparql"), queryString);
				} catch (IOException e) {
					LOGGER.error("Exception caught during saving of generated task : ", e);
				}
			}
		}
		queryIndex++;

		// QT7
		// can not be supported when we have 2 or less total versions, as there cannot exist cross-deltas
		if(enabledQueryTypes.getProperty("QT7", "0").equals("1")) {
			querySubstParamCount = 3;
			for(int querySubstParam = 1; querySubstParam <= querySubstParamCount && querySubstParam < numberOfVersions - 1; querySubstParam++) {
				queryString = compileMustacheTemplate(7, queryIndex, querySubstParam);
				tasks.add(new Task(7, 1, Integer.toString(taskId++), queryString, null));
				try {
					FileUtils.writeStringToFile(new File(queriesPath + "versioningQuery7.1." + querySubstParam + ".sparql"), queryString);
				} catch (IOException e) {
					LOGGER.error("Exception caught during saving of generated task : ", e);
				}
			}
		}
		queryIndex++;

		// QT8
		if(enabledQueryTypes.getProperty("QT8", "0").equals("1")) {
			if (numberOfVersions == 2) {
				querySubstParamCount = 1;
			} else if(numberOfVersions == 3) {
				querySubstParamCount = 3;
			} else if(numberOfVersions == 4) {
				querySubstParamCount = 5;
			} else {
				querySubstParamCount = 6;
			}
			for (int querySubType = 1; querySubType <= Statistics.VERSIONING_SUB_QUERIES_COUNT; querySubType++) {
				for(int querySubstParam = 1; querySubstParam <= querySubstParamCount; querySubstParam++) {
					queryString = compileMustacheTemplate(8, queryIndex, querySubstParam);
					tasks.add(new Task(8, querySubType, Integer.toString(taskId++), queryString, null));
					try {
						FileUtils.writeStringToFile(new File(queriesPath + "versioningQuery8." + querySubType + "." + querySubstParam + ".sparql"), queryString);
					} catch (IOException e) {
						LOGGER.error("Exception caught during saving of generated task : ", e);
					}
				}
				queryIndex++;
			}
		} else {
			queryIndex += Statistics.VERSIONING_SUB_QUERIES_COUNT;
		}
	}
	
	public void generateQuerySubstitutionParameters() {
		LOGGER.info("Generating query parameters...");
		String substitutionParametersPath = System.getProperty("user.dir") + File.separator + "substitution_parameters";
		File substitutionParametersDir = new File(substitutionParametersPath);
		substitutionParametersDir.mkdirs();

		BufferedWriter bw = null;
		Class<SubstitutionParametersGenerator> c = null;
		Constructor<?> cc = null;
		SubstitutionParametersGenerator queryTemplate = null;
		try {				
			//Versioning query parameters
			for (int i = 1; i <= Statistics.VERSIONING_QUERIES_COUNT; i++) {
				if (Arrays.asList(2,4,8).contains(i)) {
					for (int j = 1; j <= Statistics.VERSIONING_SUB_QUERIES_COUNT; j++) {
						bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(substitutionParametersPath + File.separator + String.format("versioningQuery%01d.%01dSubstParameters", i, j) + ".txt"), "UTF-8"));					
						c = (Class<SubstitutionParametersGenerator>) Class.forName(String.format("eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery%d_%dTemplate", i, j));
						cc = c.getConstructor(RandomUtil.class, HashMap.class, Definitions.class, String[].class, int[].class);
						queryTemplate = (SubstitutionParametersGenerator) cc.newInstance(randomGenerator.randomUtilFactory(configuration.getLong(Configuration.GENERATOR_RANDOM_SEED)), versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, null, dbPediaVersionsDistribution); 
						queryTemplate.generateSubstitutionParameters(bw, configuration.getInt(Configuration.QUERY_SUBSTITUTION_PARAMETERS));
						bw.close();
					}
					continue;
				}
				bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(substitutionParametersPath + File.separator + String.format("versioningQuery%01d.%01dSubstParameters", i, 1) + ".txt"), "UTF-8"));								
				c = (Class<SubstitutionParametersGenerator>) Class.forName(String.format("eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery%d_%dTemplate", i, 1));
				cc = c.getConstructor(RandomUtil.class, HashMap.class, Definitions.class, String[].class);
				queryTemplate = (SubstitutionParametersGenerator) cc.newInstance(randomGenerator.randomUtilFactory(configuration.getLong(Configuration.GENERATOR_RANDOM_SEED)), versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, null);					
				queryTemplate.generateSubstitutionParameters(bw, configuration.getInt(Configuration.QUERY_SUBSTITUTION_PARAMETERS));				
				bw.close();
			}
			LOGGER.info("Query parameters generated successfully...");
		} catch (Exception e) {
			LOGGER.error("\n\tException caught during generation of query substitution parameters : ", e);
		} finally {
			try { 
				bw.close(); 
			} catch(Exception e) { };
		}
	}
	
	/*
	 * Update SPB configuration files that are necessary for data generation
	 */
	public void reInitializeSPBProperties() {
		int numberOfGenerators = getNumberOfGenerators();
		int generatorId = getGeneratorId();
		
		LOGGER.info("Re-initializing SPB properties for Data Generator '" + generatorId + "'");
	
		majorEvents = new int[numberOfGenerators];
		minorEvents = new int[numberOfGenerators];
		correlations = new int[numberOfGenerators];
		
		// adjust the number of major/minor events and number of correlations according to
		// the total size of dataset size, in order to let the ratio of the three types of 
		// modeled data (clusterings, correlations, random) to be 33% : 33% : 33%
		AllocationsUtil au = new AllocationsUtil();
		int adjustedMajorEvents = (int) au.adjustAndGetMajorEventsAllocation(v0SizeInTriples);
		int adjustedMinorEvents = (int) au.adjustAndGetMinorEventsAllocation(v0SizeInTriples);
		int adjustedCorrelations= (int) au.adjustAndGetCorrelationsAllocation(v0SizeInTriples);
		
		// distribute the major/minor events and correlations to the data generators
		// as each generator has to produce the whole event/correlation in order to be valid
		int standarMajorEventsPerGenerator = adjustedMajorEvents / numberOfGenerators;
		int additionalMajorEventsPerGenerator = adjustedMajorEvents % numberOfGenerators;
		for(int i = 0; i < majorEvents.length; i++, additionalMajorEventsPerGenerator--) {
			majorEvents[i] = standarMajorEventsPerGenerator; 
			majorEvents[i] += additionalMajorEventsPerGenerator > 0 ? 1 : 0;
		}
		
		int standarMinorEventsPerGenerator = adjustedMinorEvents / numberOfGenerators;
		int additionalMinorEventsPerGenerator = adjustedMinorEvents % numberOfGenerators;
		for(int i = 0; i < minorEvents.length; i++, additionalMinorEventsPerGenerator--) {
			minorEvents[i] = standarMinorEventsPerGenerator; 
			minorEvents[i] += additionalMinorEventsPerGenerator > 0 ? 1 : 0;
		}
		
		int standarCorrelationsPerGenerator = adjustedCorrelations / numberOfGenerators;
		int additionalCorrelationsPerGenerator = adjustedCorrelations % numberOfGenerators;
		for(int i = 0; i < correlations.length; i++, additionalCorrelationsPerGenerator--) {
			correlations[i] = standarCorrelationsPerGenerator; 
			correlations[i] += additionalCorrelationsPerGenerator > 0 ? 1 : 0;
		}
		
		// get the number of major/minor events as the number of correlations that have to be 
		// produced by the current data generator 
		int currDataGeneratorMajorEvents = majorEvents[generatorId];
		int currDataGeneratorMinorEvents = minorEvents[generatorId];
		int currDataGeneratorCorrelations = correlations[generatorId];

		
		// calculate the total size of triples that have to be produced by the current
		// data generator. Such quantity is based on the previously computed number of 
		// major/minor events and correlations where it is necessary.
		// ~500K triples per major event
		// ~50K triples per minor events
		// ~160K triples per correlation
		// The data generators with higher ids may have to generate no data 
		int triplesBasedOnEvents = currDataGeneratorMajorEvents * 500000 + currDataGeneratorMinorEvents * 50000 + currDataGeneratorCorrelations * 160000;
		int triplesBasedOnDataGenerators = v0SizeInTriples / numberOfGenerators;
		int currDataGeneratorDatasetSizeInTriples = triplesBasedOnEvents > triplesBasedOnDataGenerators ? triplesBasedOnEvents : triplesBasedOnDataGenerators;

		// compute the approximated number of triples that have been generated so far
		int approxGeneratedTriplesSoFar = 0;
		for(int i = 0; i < generatorId; i++) {
			int prevTriplesBasedOnEvents = majorEvents[i] * 500000 + minorEvents[i] * 50000 + correlations[i] * 160000;
			approxGeneratedTriplesSoFar += prevTriplesBasedOnEvents > triplesBasedOnDataGenerators ? prevTriplesBasedOnEvents : triplesBasedOnDataGenerators;
		}
		
		// if the difference between the total size of triples and the approximated number of
		// triples so far is lower than the number of triples that each data generator have to
		// produce, let the number of generated triples to be such a difference.
		currDataGeneratorDatasetSizeInTriples = (approxGeneratedTriplesSoFar + currDataGeneratorDatasetSizeInTriples) >  v0SizeInTriples ? v0SizeInTriples - approxGeneratedTriplesSoFar : currDataGeneratorDatasetSizeInTriples;
		// if the number of generated triples so far has reached the target, let the current data generator 
		// to produce no triples			
		currDataGeneratorDatasetSizeInTriples = approxGeneratedTriplesSoFar > v0SizeInTriples ? 0 : currDataGeneratorDatasetSizeInTriples;
		
		LOGGER.info("Generator '" + generatorId + "' will produce "+ (currDataGeneratorDatasetSizeInTriples > 0 ? "~" : "") 
				+ currDataGeneratorDatasetSizeInTriples + " triples,"
				+ " composed of:"
					+ "\n\t\t\t" + currDataGeneratorMajorEvents + " major events of total " + adjustedMajorEvents
					+ "\n\t\t\t" + currDataGeneratorMinorEvents + " minor events of total " + adjustedMinorEvents
					+ "\n\t\t\t" + currDataGeneratorCorrelations + " correlations of total " + adjustedCorrelations);

		// get available cores to let data generated through multiple threads. 
//		dataGeneratorWorkers = Runtime.getRuntime().availableProcessors() / 2;
		
		// re-initialize test.properties file that is required for data generation
		configuration.setIntProperty("datasetSize", currDataGeneratorDatasetSizeInTriples);
		configuration.setIntProperty("numberOfVersions", numberOfVersions);
		configuration.setIntProperty("generatorRandomSeed", subGeneratorSeed);
		configuration.setIntProperty("hobbitDataGeneratorId", generatorId);
		configuration.setStringProperty("creativeWorksPath", initialVersionDataPath);
		
		// re-initialize generorPeriodYears according to the total number of versions and the 
		// version times of dbpedia data ('12, '13, '14, '15, '16)
		generorPeriodYears = numberOfVersions < 5 ? numberOfVersions : 5;

		// re-initialize definitions.properties file that is required for data generation
		definitions.setIntProperty("dataGenerationPeriodYears", generorPeriodYears);
		definitions.setIntProperty("majorEvents", currDataGeneratorMajorEvents);
		definitions.setIntProperty("minorEvents", currDataGeneratorMinorEvents);
		definitions.setIntProperty("correlationsAmount", currDataGeneratorCorrelations);
	}
	
	/**
     * A generic method for initialize benchmark parameters from environment variables
     * 
     * @param env		a map of all available environment variables
     * @param parameter	the property that we want to get
     * @param paramType	a dummy parameter to recognize property's type
     */
	@SuppressWarnings("unchecked")
	private <T> T getFromEnv(Map<String, String> env, String parameter, T paramType) {
		if (!env.containsKey(parameter)) {
			LOGGER.error(
					"Environment variable \"" + parameter + "\" is not set. Aborting.");
            throw new IllegalArgumentException(
            		"Environment variable \"" + parameter + "\" is not set. Aborting.");
        }
		try {
			if (paramType instanceof String) {
				return (T) env.get(parameter);
			} else if (paramType instanceof Integer) {
				return (T) (Integer) Integer.parseInt(env.get(parameter));
			} else if (paramType instanceof Long) {
				return (T) (Long) Long.parseLong(env.get(parameter));
			} else if (paramType instanceof Double) {
				return (T) (Double) Double.parseDouble(env.get(parameter));
			}
        } catch (Exception e) {
        	throw new IllegalArgumentException(
                    "Couldn't get \"" + parameter + "\" from the environment. Aborting.", e);
        }
		return paramType;
	}
		
	/*
	 * Retreive entity URIs, DBpedia locations IDs and Geonames locations IDs
	 * of reference datasets from the appropriate files.
	 */
	private void populateRefDataEntitiesListsFromFiles() throws IOException {
		String entitiesFullPath = System.getProperty("user.dir") + File.separator + "entities_1000.txt";
		String dbpediaLocationsFullPathName = System.getProperty("user.dir") + File.separator + "dbpediaLocations.txt";
		String geonamesFullPathName = System.getProperty("user.dir") + File.separator + "geonamesIDs.txt";
		
		//retrieve entity URIs from the appropriate file
		ReferenceDataAnalyzer refDataAnalyzer = new ReferenceDataAnalyzer();
		ArrayList<Entity> entitiesList = refDataAnalyzer.initFromFile(entitiesFullPath);

		// assuming that popular entities correspond to 5% of total entities
		long popularEntitiesCount = (int) (entitiesList.size() * 0.05);
		
		for (int i = 0; i < entitiesList.size(); i++) {
			Entity e = entitiesList.get(i);
			if (i <= popularEntitiesCount) {
				DataManager.popularEntitiesList.add(e);
			} else {
				DataManager.regularEntitiesList.add(e);
			}
		}
		
		//retrieve DBpedia locations IDs from the appropriate file
		LocationsAnalyzer gna = new LocationsAnalyzer();
		ArrayList<String> locationsIds = gna.initFromFile(dbpediaLocationsFullPathName);

		for (String s : locationsIds) {
			DataManager.locationsIdsList.add(s);
		}
			
		locationsIds.clear();
		
		//retrieve Geonames locations IDs from the appropriate file
		gna = new LocationsAnalyzer();
		locationsIds = gna.initFromFile(geonamesFullPathName);

		for (String s : locationsIds) {
			DataManager.geonamesIdsList.add(s);
		}
	}
	
	@Override
	// This method is used for sending the already generated data, tasks and gold standard
	// to the appropriate components.
	protected void generateData() throws Exception {
		try {						
			// Send data files to the system.
			// Starting for version 0 and continue to the next one until reaching the last version.
			// Waits for signal sent by the system that determines that the sent version successfully 
			// loaded, in order to proceed with the next one
	    	for(int version = 0; version < numberOfVersions; version++) {
	    		numberOfmessages.set(0);
	    		// if the benchmark is configured by the system to send change sets
	    		if(sentDataForm.equals("cs") || sentDataForm.equals("both")) {
	    			if(version == 0) {
		    			// TODO if multiple data generators will be supported 
		    			// ontologies have to be sent only from one data generator
		    			// send ontology files to the system
		    			File ontologiesPathFile = new File(ontologiesPath);
		    			List<File> ontologiesFiles = (List<File>) FileUtils.listFiles(ontologiesPathFile, new String[] { "nt" }, true);
		    			for (File file : ontologiesFiles) {
		    				String graphUri = "http://datagen.addset." + version + "." + file.getName();
		    				byte data[] = FileUtils.readFileToByteArray(file);
		    				byte[] dataForSending = RabbitMQUtils.writeByteArrays(null, new byte[][]{RabbitMQUtils.writeString(graphUri)}, data);
		    				sendDataToSystemAdapter(dataForSending);
		    				numberOfmessages.incrementAndGet();
		    			}
		    	    	LOGGER.info("All ontologies successfully sent to System Adapter.");
		    		}
	    			
	    			File dataPath = new File(generatedDatasetPath + File.separator + (version == 0 ? "v" : "c") + version);
	    			if(!dataPath.exists()) dataPath.mkdirs();
	    			List<File> addedDataFiles = (List<File>) FileUtils.listFiles(dataPath, new String[] { "added.nt" }, false);
	    			for (File file : addedDataFiles) {
	    				String graphUri = "http://datagen.addset." + version + "." + file.getName();
	    				byte data[] = FileUtils.readFileToByteArray(file);
	    				byte[] dataForSending = RabbitMQUtils.writeByteArrays(null, new byte[][]{RabbitMQUtils.writeString(graphUri)}, data);
	    				sendDataToSystemAdapter(dataForSending);
	    				numberOfmessages.incrementAndGet();
	    			}
	    			List<File> deletedDataFiles = (List<File>) FileUtils.listFiles(dataPath, new String[] { "deleted.nt" }, false);
	    			for (File file : deletedDataFiles) {
	    				String graphUri = "http://datagen.deleteset." + version + "." + file.getName();
	    				byte data[] = FileUtils.readFileToByteArray(file);
	    				byte[] dataForSending = RabbitMQUtils.writeByteArrays(null, new byte[][]{RabbitMQUtils.writeString(graphUri)}, data);
	    				sendDataToSystemAdapter(dataForSending);
	    				numberOfmessages.incrementAndGet();
	    			}
	    		} else if(sentDataForm.equals("ic") || sentDataForm.equals("both")) {
		    		// if the benchmark is configured by the system to send independent copy of each version.
	    			// send the final data that previously computed when computed expected answers.
	    			File dataPath = new File(generatedDatasetPath + File.separator + "final" + File.separator + "v" + version);
	    			List<File> dataFiles = (List<File>) FileUtils.listFiles(dataPath, new String[] { "nt" }, false);
	    			for (File file : dataFiles) {
	    				String graphUri = "http://datagen.version." + version + "." + file.getName();
	    				byte data[] = FileUtils.readFileToByteArray(file);
	    				byte[] dataForSending = RabbitMQUtils.writeByteArrays(null, new byte[][]{RabbitMQUtils.writeString(graphUri)}, data);
	    				sendDataToSystemAdapter(dataForSending);
	    				numberOfmessages.incrementAndGet();
	    			}
	    		}
    	    	LOGGER.info("Generated data for version " + version + " successfully sent to System Adapter.");
			
    	    	// TODO: sending of such signal when data were generated and not when data sent to
            	// sysada. a new signal has to be sent in such cases (do it when expected answers 
            	// computation removed from datagen implementation) now cannot to be done as 
            	// all data generated in the init function (before datagen's start)
            	LOGGER.info("Send signal to benchmark controller that all data (#" + numberOfmessages + ") of version " + version +" successfully sent to system adapter.");
            	ByteBuffer buffer = ByteBuffer.allocate(20);
            	buffer.putInt(triplesExpectedToBeAdded[version]);
            	buffer.putInt(triplesExpectedToBeDeleted[version]);
            	buffer.putInt(triplesExpectedToBeLoaded[version]);
            	buffer.putInt(getGeneratorId());
            	buffer.putInt(numberOfmessages.get());
    			sendToCmdQueue(VersioningConstants.DATA_GEN_VERSION_DATA_SENT, buffer.array());

    			LOGGER.info("Waiting until system receive and load the sent data.");
    			versionLoadedFromSystemMutex.acquire();
	    	}
		} catch (Exception e) {
            LOGGER.error("Exception while sending generated data to System Adapter.", e);
        }
		
        try {
        	// send generated tasks along with their expected answers to task generator
        	for (Task task : tasks) {
        		byte[] data = SerializationUtils.serialize(task);       			
    			sendDataToTaskGenerator(data);
            	LOGGER.info("Task " + task.getTaskId() + " sent to Task Generator.");
        	}
        	LOGGER.info("All generated tasks successfully sent to Task Generator.");
        } catch (Exception e) {
            LOGGER.error("Exception while sending tasks to Task Generator.", e);
        }
	}

	// method for loading to virtuoso the first N versions. e.g. for first 2 versions
	// v0 and v1 will be loaded into.
	public void loadFirstNVersions(int n) {
		LOGGER.info("Loading generated data, up to version " + n + ", to Virtuoso triplestore...");

		try {
			String scriptFilePath = System.getProperty("user.dir") + File.separator + "load_to_virtuoso.sh";
			String[] command = {"/bin/bash", scriptFilePath, RDFUtils.getFileExtensionFromRdfFormat(serializationFormat), Integer.toString(n) };
			Process p = new ProcessBuilder(command).start();
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
			BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			String line;
			while ((line = stdInput.readLine()) != null) {
				LOGGER.info(line);
			}
			while ((line = stdError.readLine()) != null) {
				LOGGER.info(line);
			}
			p.waitFor();
			LOGGER.info("Generated data loaded successfully.");
			stdInput.close();
			stdError.close();
		} catch (IOException e) {
            LOGGER.error("Exception while executing script for loading data.", e);
		} catch (InterruptedException e) {
            LOGGER.error("Exception while executing script for loading data.", e);
		}		
	}
	
	// method for loading to virtuoso a specific version
	public void loadVersion(int version) {
		LOGGER.info("Loading generated data, for version " + version + ", to Virtuoso triplestore...");

		try {
			String scriptFilePath = System.getProperty("user.dir") + File.separator + "load_version_to_virtuoso.sh";
			String[] command = {"/bin/bash", scriptFilePath, RDFUtils.getFileExtensionFromRdfFormat(serializationFormat), Integer.toString(version) };
			Process p = new ProcessBuilder(command).start();
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ((line = in.readLine()) != null) {
				LOGGER.info(line);
			}
			p.waitFor();
			LOGGER.info("Generated data loaded successfully.");
			in.close();
		} catch (IOException e) {
            LOGGER.error("Exception while executing script for loading data.", e);
		} catch (InterruptedException e) {
            LOGGER.error("Exception while executing script for loading data.", e);
		}		
	}

	private static void extractDeleted(String currentFile, String cwIdsFile, String destPath) {
		try {
			String scriptFilePath = System.getProperty("user.dir") + File.separator + "export_cws_tbd.sh";
			String[] command = {"/bin/bash", scriptFilePath, currentFile, cwIdsFile, destPath };
			Process p = new ProcessBuilder(command).start();
			BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			String line = null;
			while ((line = stdError.readLine()) != null) {
				LOGGER.info(line);
			}
			p.waitFor();
			stdError.close();
		} catch (IOException e) {
            LOGGER.error("Exception while executing script for extracting creative works that have to be deleted.", e);
		} catch (InterruptedException e) {
            LOGGER.error("Exception while executing script for extracting creative works that have to be deleted.", e);
		}	
	}	
	@Override
    public void receiveCommand(byte command, byte[] data) {
        if (command == SystemAdapterConstants.BULK_LOADING_DATA_FINISHED) {
        	versionLoadedFromSystemMutex.release();
        }
        super.receiveCommand(command, data);
    }
	
	public void writeResults() {
		String resultsPath = System.getProperty("user.dir") + File.separator + "results";
		File resultsDir = new File(resultsPath);
		resultsDir.mkdirs();
		// skip current version materialization query
		int taskId = 0;
		
		// mind the non zero-based numbering of query types 
		for (int queryType = 0; queryType < Statistics.VERSIONING_QUERIES_COUNT; queryType++) {
			if (Arrays.asList(1,3,7).contains(queryType)) {
				for (int querySubType = 0; querySubType < Statistics.VERSIONING_SUB_QUERIES_COUNT; querySubType++) {	
					for (int querySubstParam = 0; querySubstParam < subsParametersAmount; querySubstParam++) {
						ByteBuffer expectedResultsBuffer = ByteBuffer.wrap(tasks.get(taskId++).getExpectedAnswers());
						expectedResultsBuffer.getInt();
						expectedResultsBuffer.getInt();
						byte expectedDataBytes[] = RabbitMQUtils.readByteArray(expectedResultsBuffer);
						try {
							FileUtils.writeByteArrayToFile(new File(resultsDir + File.separator + "versionigQuery" + (queryType + 1) + "." + (querySubType + 1) + "." + (querySubstParam + 1) + "_results.json"), expectedDataBytes);
						} catch (IOException e) {
							LOGGER.error("Exception caught during saving of expected results: ", e);
						}
					}
				}
				continue;
			}
			for (int querySubstParam = 0; querySubstParam < subsParametersAmount; querySubstParam++) {
				ByteBuffer expectedResultsBuffer = ByteBuffer.wrap(tasks.get(taskId++).getExpectedAnswers());
				expectedResultsBuffer.getInt();
				expectedResultsBuffer.getInt();
				byte expectedDataBytes[] = RabbitMQUtils.readByteArray(expectedResultsBuffer);
				try {
					FileUtils.writeByteArrayToFile(new File(resultsDir + File.separator + "versionigQuery" + (queryType + 1) + ".1." + (querySubstParam + 1) + "_results.json"), expectedDataBytes);
				} catch (IOException e) {
					LOGGER.error("Exception caught during saving of expected results : ", e);
				}
				if (queryType == 0) break;
			}
		}
	}

	public void sendAllToFTP() {
		writeResults();
		FTPUtils.sendToFtp("/versioning/data/v0/", "public/MOCHA_ESWC2018/Task3/data/changesets/c0", "nt");
		FTPUtils.sendToFtp("/versioning/data/c1/", "public/MOCHA_ESWC2018/Task3/data/changesets/c1", "nt");
		FTPUtils.sendToFtp("/versioning/data/c2/", "public/MOCHA_ESWC2018/Task3/data/changesets/c2", "nt");
		FTPUtils.sendToFtp("/versioning/data/c3/", "public/MOCHA_ESWC2018/Task3/data/changesets/c3", "nt");
		FTPUtils.sendToFtp("/versioning/data/c4/", "public/MOCHA_ESWC2018/Task3/data/changesets/c4", "nt");
		FTPUtils.sendToFtp("/versioning/data/final/v0/", "public/MOCHA_ESWC2018/Task3/data/independentcopies/v0", "nt");
		FTPUtils.sendToFtp("/versioning/data/final/v1/", "public/MOCHA_ESWC2018/Task3/data/independentcopies/v1", "nt");
		FTPUtils.sendToFtp("/versioning/data/final/v2/", "public/MOCHA_ESWC2018/Task3/data/independentcopies/v2", "nt");
		FTPUtils.sendToFtp("/versioning/data/final/v3/", "public/MOCHA_ESWC2018/Task3/data/independentcopies/v3", "nt");
		FTPUtils.sendToFtp("/versioning/data/final/v4/", "public/MOCHA_ESWC2018/Task3/data/independentcopies/v4", "nt");
		FTPUtils.sendToFtp("/versioning/queries/", "public/MOCHA_ESWC2018/Task3/queries", "sparql");
		FTPUtils.sendToFtp("/versioning/query_templates/", "public/MOCHA_ESWC2018/Task3/query_templates", "txt");
		FTPUtils.sendToFtp("/versioning/substitution_parameters/", "public/MOCHA_ESWC2018/Task3/substitution_parameters", "txt");
		FTPUtils.sendToFtp("/versioning/results/", "public/MOCHA_ESWC2018/Task3/expected_results", "json");
	}
	
	@Override
	public void close() throws IOException {
		LOGGER.info("Closing Data Generator...");
        super.close();
		LOGGER.info("Data Generator closed successfully.");
    }
}
