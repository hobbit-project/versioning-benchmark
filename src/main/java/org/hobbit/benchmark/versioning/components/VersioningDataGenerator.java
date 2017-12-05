package org.hobbit.benchmark.versioning.components;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.hobbit.benchmark.versioning.Task;
import org.hobbit.benchmark.versioning.properties.RDFUtils;
import org.hobbit.benchmark.versioning.properties.VersioningConstants;
import org.hobbit.benchmark.versioning.util.VirtuosoSystemAdapterConstants;
import org.hobbit.core.components.AbstractDataGenerator;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

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
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery1_1Template;
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery2_1Template;
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery2_2Template;
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery2_3Template;
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery2_4Template;
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery3_1Template;
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery4_1Template;
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery4_2Template;
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery4_3Template;
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery4_4Template;
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery5_1Template;
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery6_1Template;
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery7_1Template;
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery8_1Template;
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery8_2Template;
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery8_3Template;
import eu.ldbc.semanticpublishing.templates.versioning.VersioningQuery8_4Template;
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
	private String generatedDatasetPath = "/versioning/data";
	private String initialVersionDataPath = generatedDatasetPath + File.separator + "v0";
	private String ontologiesPath = "/versioning/ontologies";
	private String dbpediaPath = "/versioning/dbpedia";
	private int taskId = 0;
	private int[] triplesExpectedToBeLoaded;
	private int[] cwsToBeLoaded;
	
	private AtomicInteger numberOfmessages = new AtomicInteger(0);
	
	private Configuration configuration = new Configuration();
	private Definitions definitions = new Definitions();
	private RandomUtil randomGenerator = null;
	
	private VersioningMustacheTemplatesHolder versioningMustacheTemplatesHolder = new VersioningMustacheTemplatesHolder();
	private SubstitutionQueryParametersManager substitutionQueryParamtersManager = new SubstitutionQueryParametersManager();
	
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
		
		configuration.loadFromFile(configurationFile); 
		definitions.loadFromFile(definitionsFile, configuration.getBoolean(Configuration.VERBOSE)); 
		
		maxTriplesPerFile = configuration.getInt(Configuration.GENERATED_TRIPLES_PER_FILE);
		dataGeneratorWorkers = configuration.getInt(Configuration.DATA_GENERATOR_WORKERS);
		subsParametersAmount = configuration.getInt(Configuration.QUERY_SUBSTITUTION_PARAMETERS);
		serializationFormat = configuration.getString(Configuration.GENERATE_CREATIVE_WORKS_FORMAT);
		seedYear = definitions.getInt(Definitions.YEAR_SEED);
		
		// Initialize data generation parameters through the environment variables given by user
		initFromEnv();
		triplesExpectedToBeLoaded = new int[numberOfVersions];
		cwsToBeLoaded = new int[numberOfVersions];
		
		// Given the above input, update configuration files that are necessary for data generation
		reInitializeSPBProperties();
		
		randomGenerator = new RandomUtil(dictionaryFile, subGeneratorSeed, seedYear, generorPeriodYears);
		definitions.initializeAllocations(randomGenerator.getRandom());
	
		// Set the nextId for Creative Works
		DataManager.creativeWorksNextId.set(configuration.getLong(Configuration.CREATIVE_WORK_NEXT_ID));

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
		
		
		// Generate the change sets. Only additions/deletions are supported.
		// TODO: support changes
		int preVersionDeletedCWs = 0;
		long changeSetStart = System.currentTimeMillis();
		for(int i = 1; i < numberOfVersions; i++) {
			int triplesToBeAdded = Math.round(versionInsertionRatio / 100f * cwsToBeLoaded[i-1]);
			int triplesToBeDeleted = Math.round(versionDeletionRatio / 100f * cwsToBeLoaded[i-1]);
			cwsToBeLoaded[i] = cwsToBeLoaded[i-1] + triplesToBeAdded - triplesToBeDeleted;
			LOGGER.info("Generating version " + i + " changeset. Target: " + "[+" + triplesToBeAdded + ", -" + triplesToBeDeleted + "]");
			String destinationPath = generatedDatasetPath + File.separator + "c" + i;
			
			// produce the add set
			dataGenerator.produceAdded(destinationPath, triplesToBeAdded);
			
			// produce the delete set
			long deleteSetStart = System.currentTimeMillis();
			int currVersionDeletedCreativeWorks = 0;
			int currVersionDeletedTriples = 0;
			int creativeWorkAvgTriples = DataManager.randomTriples.intValue() / DataManager.randomCreativeWorkIdsList.size();

			// Estimate the total number of creative works that have to be deleted, using 
			// creative work average triples that have been generated so far.
			int creativeWorksToBeDeleted = triplesToBeDeleted / creativeWorkAvgTriples;
			LOGGER.info("Initial estimation: " + creativeWorksToBeDeleted + " cworks have to be deleted from v" + (i-1));
			while (currVersionDeletedTriples < triplesToBeDeleted) {
				ArrayList<String> cwToBeDeleted = new ArrayList<String>();
				for(int c=0; c<creativeWorksToBeDeleted; c++) {
					int deletedCWIndex = randomGenerator.nextInt(DataManager.remainingRandomCreativeWorkIdsList.size() - 1);
					long creativeWorkToBeDeleted = DataManager.remainingRandomCreativeWorkIdsList.get(deletedCWIndex);
					DataManager.remainingRandomCreativeWorkIdsList.remove(deletedCWIndex);
					cwToBeDeleted.add("http://www.bbc.co.uk/things/" + getGeneratorId() + "-" + creativeWorkToBeDeleted + "#id");
				}
				FileUtils.writeLines(new File("/versioning/creativeWorksToBeDeleted.txt") , cwToBeDeleted, false);
				int deletedTriples = 0;
				for(int j=0; j<i; j++) {
					String sourcePath = generatedDatasetPath + File.separator + (j == 0 ? "v" : "c") + j;
					deletedTriples = extractDeleted(sourcePath, "/versioning/creativeWorksToBeDeleted.txt", destinationPath);
					currVersionDeletedTriples += deletedTriples;
				}
				currVersionDeletedCreativeWorks += creativeWorksToBeDeleted;
				// estimation of the remaining creative works that have to be extracted
				creativeWorksToBeDeleted = (int) Math.ceil((double) (triplesToBeDeleted - currVersionDeletedTriples) / creativeWorkAvgTriples);
				if(creativeWorksToBeDeleted > 0) {
					LOGGER.info("Estimation: " + creativeWorksToBeDeleted + " more cwork" + (creativeWorksToBeDeleted > 1 ? "s" : "") +" have to be deleted.");
				}
			}
			preVersionDeletedCWs = currVersionDeletedCreativeWorks;
			long deleteSetEnd = System.currentTimeMillis();
			LOGGER.info("Deleteset of total " + preVersionDeletedCWs + " Creative Works generated successfully. Triples: " + currVersionDeletedTriples + ". Target: " + triplesToBeDeleted + " triples. Time: " + (deleteSetEnd - deleteSetStart) + " ms.");
		}
		long changeSetEnd = System.currentTimeMillis();
		LOGGER.info("All changesets generated successfully. Time: " + (changeSetEnd - changeSetStart) + " ms.");

		// Evenly distribute the 5 dbpedia versions to the total number of versions that were generated
		distributeDBpediaVersions();

		LOGGER.info("Generating tasks...");
		// 3) Generate SPARQL query tasks
		// generate substitution parameters
		String queriesPath = System.getProperty("user.dir") + File.separator + "query_templates";
		versioningMustacheTemplatesHolder.loadFrom(queriesPath);
		generateQuerySubstitutionParameters();
		
		// initialize substitution parameters
		String substitutionParametersPath = System.getProperty("user.dir") + File.separator + "substitution_parameters";
		LOGGER.info("Initializing parameters for SPARQL query tasks...");
		substitutionQueryParamtersManager.intiVersioningSubstitutionParameters(substitutionParametersPath, false, false);
		LOGGER.info("Query parameters initialized successfully.");


		// build mustache templates to create queries
		LOGGER.info("Building SPRQL tasks...");
		buildSPRQLTasks();
		LOGGER.info("All SPRQL tasks built successfully.");

		LOGGER.info("Loading generating data, in order to compute gold standard...");
		// load generated creative works to virtuoso, in order to compute the gold standard
		loadFirstNVersions(numberOfVersions);
		
		// compute expected answers for all tasks
		LOGGER.info("Computing expected answers for generated SPARQL tasks...");
		computeExpectedAnswers();
		LOGGER.info("Expected answers have computed successfully for all generated SPRQL tasks.");	
	}
	
	public void initFromEnv() {
		LOGGER.info("Getting Data Generator's properites from the environment...");
		
		Map<String, String> env = System.getenv();
		v0SizeInTriples = (Integer) getFromEnv(env, VersioningConstants.V0_SIZE_IN_TRIPLES, 0);
		numberOfVersions = (Integer) getFromEnv(env, VersioningConstants.NUMBER_OF_VERSIONS, 0);
		subGeneratorSeed = (Integer) getFromEnv(env, VersioningConstants.DATA_GENERATOR_SEED, 0) + getGeneratorId();
		versionInsertionRatio = (Integer) getFromEnv(env, VersioningConstants.VERSION_INSERTION_RATIO, 5);
		versionDeletionRatio = (Integer) getFromEnv(env, VersioningConstants.VERSION_DELETION_RATIO, 3);
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
		dbPediaVersionsDistribution = new int[numberOfVersions];
		double dbpediaVersions = 5;
		int versionsToMap = 5;
		double step = (numberOfVersions / dbpediaVersions) < 1 ? Math.floor(numberOfVersions / dbpediaVersions) : Math.ceil(numberOfVersions / dbpediaVersions);
		Arrays.fill(dbPediaVersionsDistribution, step == 0 ? 1 : 0);
		
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

		int dbpediaIndex = 0;
		
		while(versionsToMap > 0 && step > 0) {
			for (int i=0; i<dbPediaVersionsDistribution.length;) {
				if(versionsToMap == 0) {
					break;
				} else if(dbPediaVersionsDistribution[i] == 1) {
					i += step/2;
				} else {
					dbPediaVersionsDistribution[i] = 1;
					// copy the dbpedia file to the appropriate version dir, determined by the index i
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
					} catch(IOException e) {
						LOGGER.error("Exception caught during the copy of dbpedia files to the appropriate version dir", e);
					}
					i += step;
					versionsToMap--;
					dbpediaIndex++;
				}
			}
		}
		
	}

	
	// get the query strings after compiling the mustache templates
	public String compileMustacheTemplate(int queryType, int queryIndex, int subsParameterIndex) {
		String[] querySubstParameters = substitutionQueryParamtersManager.getSubstitutionParametersFor(SubstitutionQueryParametersManager.QueryType.VERSIONING, queryIndex).get(subsParameterIndex);

		String compiledQuery = null;
		switch (queryType) {
			case 0 :
				MustacheTemplate versioningQuery1_1 = new VersioningQuery1_1Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
				compiledQuery = versioningQuery1_1.compileMustacheTemplate();
				break;
			case 1 :
				if(queryIndex == 1) {
					MustacheTemplate versioningQuery2_1 = new VersioningQuery2_1Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
					compiledQuery = versioningQuery2_1.compileMustacheTemplate();
					break;
				} else if (queryIndex == 2) {
					MustacheTemplate versioningQuery2_2 = new VersioningQuery2_2Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
					compiledQuery = versioningQuery2_2.compileMustacheTemplate();
					break;
				} else if (queryIndex == 3) {
					MustacheTemplate versioningQuery2_3 = new VersioningQuery2_3Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
					compiledQuery = versioningQuery2_3.compileMustacheTemplate();
					break;
				} else if (queryIndex == 4) {
					MustacheTemplate versioningQuery2_4 = new VersioningQuery2_4Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
					compiledQuery = versioningQuery2_4.compileMustacheTemplate();
					break;
				}
			case 2 : 
				MustacheTemplate versioningQuery3_1 = new VersioningQuery3_1Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
				compiledQuery = versioningQuery3_1.compileMustacheTemplate();
				break;
			case 3 :
				if(queryIndex == 6) {
					MustacheTemplate versioningQuery4_1 = new VersioningQuery4_1Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
					compiledQuery = versioningQuery4_1.compileMustacheTemplate();
					break;
				} else if (queryIndex == 7) {
					MustacheTemplate versioningQuery4_2 = new VersioningQuery4_2Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
					compiledQuery = versioningQuery4_2.compileMustacheTemplate();
					break;
				} else if (queryIndex == 8) {
					MustacheTemplate versioningQuery4_3 = new VersioningQuery4_3Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
					compiledQuery = versioningQuery4_3.compileMustacheTemplate();
					break;
				} else if (queryIndex == 9) {
					MustacheTemplate versioningQuery4_4 = new VersioningQuery4_4Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
					compiledQuery = versioningQuery4_4.compileMustacheTemplate();
					break;
				}
			case 4 :
				MustacheTemplate versioningQuery5_1 = new VersioningQuery5_1Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
				compiledQuery = versioningQuery5_1.compileMustacheTemplate();
				break;
			case 5 : 
				MustacheTemplate versioningQuery6_1 = new VersioningQuery6_1Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
				compiledQuery = versioningQuery6_1.compileMustacheTemplate();
				break;
			case 6 :
				MustacheTemplate versioningQuery7_1 = new VersioningQuery7_1Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
				compiledQuery = versioningQuery7_1.compileMustacheTemplate();
				break;			
			case 7 :
				if(queryIndex == 13) {
					MustacheTemplate versioningQuery8_1 = new VersioningQuery8_1Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
					compiledQuery = versioningQuery8_1.compileMustacheTemplate();
					break;
				} else if (queryIndex == 14) {
					MustacheTemplate versioningQuery8_2 = new VersioningQuery8_2Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
					compiledQuery = versioningQuery8_2.compileMustacheTemplate();
					break;
				} else if (queryIndex == 15) {
					MustacheTemplate versioningQuery8_3 = new VersioningQuery8_3Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
					compiledQuery = versioningQuery8_3.compileMustacheTemplate();
					break;
				} else if (queryIndex == 16) {
					MustacheTemplate versioningQuery8_4 = new VersioningQuery8_4Template(randomGenerator, versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, querySubstParameters);
					compiledQuery = versioningQuery8_4.compileMustacheTemplate();
					break;
				}
		}
		return compiledQuery;
	}
	
	private int getVersionSize(int versionNum) {
		int triplesNum = 0;
		String sparqlQueryString = ""
				+ "SELECT (COUNT(*) AS ?cnt) "
				+ "FROM <http://graph.version." + versionNum + "> "
				+ "WHERE { ?s ?p ?o }";
		
		Query countQuery = QueryFactory.create(sparqlQueryString);
		QueryExecution cQexec = QueryExecutionFactory.sparqlService("http://localhost:8891/sparql", countQuery);
		ResultSet results = cQexec.execSelect();
		if(results.hasNext()) {
			triplesNum = results.next().getLiteral("cnt").getInt();
		}
		return triplesNum;
	}
	
	public void computeExpectedAnswers() {	
		// compute the number of triples that expected to be loaded by the system.
		// so the evaluation module can compute the ingestion and average changes speeds
		for (int version = 0; version < numberOfVersions; version++) {
			triplesExpectedToBeLoaded[version] = getVersionSize(version);
		}
		
		for (Task task : tasks) {
			String taskId = task.getTaskId();
			String taskQuery = task.getQuery();
			int queryType = task.getQueryType();
			
			long queryStart = 0;
			long queryEnd = 0;
			ResultSet results = null;
			
			byte[] expectedAnswers = null;
			
			// execute the query on top of virtuoso to compute the expected answers
			Query query = QueryFactory.create(taskQuery);
			QueryExecution qexec = QueryExecutionFactory.sparqlService("http://localhost:8891/sparql", query);
			queryStart = System.currentTimeMillis();
			try {
				results = qexec.execSelect();
			} catch (Exception e) {
				LOGGER.error("Exception caught during the computation of task " + taskId + " expected answers.", e);
			}				
			queryEnd = System.currentTimeMillis();

			// update the task by setting its expected results
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			ResultSetFormatter.outputAsJSON(outputStream, results);
			expectedAnswers = outputStream.toByteArray();
			//debug
			LOGGER.info("Expected answers for task " + taskId + " computed. "
					+ "Type: " + queryType 
					+ ", ResultsNum: " + results.getRowNumber() 
					+ ", Time: " + (queryEnd - queryStart) + " ms.");			

			task.setExpectedAnswers(expectedAnswers);
			tasks.set(Integer.parseInt(taskId), task);
			qexec.close();
		}	
	}
	
	public void writeResults() {
		String resultsPath = System.getProperty("user.dir") + File.separator + "results";
		File resultsDir = new File(resultsPath);
		resultsDir.mkdirs();
		int taskId = 1;
		
		// mind the non zero-based numbering of query types 
		for (int queryType = 0; queryType < Statistics.VERSIONING_QUERIES_COUNT; queryType++) {
			if (Arrays.asList(1,3,7).contains(queryType)) {
				for (int querySubType = 0; querySubType < Statistics.VERSIONING_SUB_QUERIES_COUNT; querySubType++) {	
					for (int querySubstParam = 0; querySubstParam < subsParametersAmount; querySubstParam++) {
						ByteBuffer expectedResultsBuffer = ByteBuffer.wrap(tasks.get(taskId++).getExpectedAnswers());
						RabbitMQUtils.readString(expectedResultsBuffer);
						byte[] expectedResults = RabbitMQUtils.readString(expectedResultsBuffer).getBytes(StandardCharsets.UTF_8);
						try {
							FileUtils.writeByteArrayToFile(new File(resultsDir + File.separator + "versionigQuery" + (queryType + 1) + "." + (querySubType + 1) + "." + (querySubstParam + 1) + "_results.json"), expectedResults);
						} catch (IOException e) {
							LOGGER.error("Exception caught during saving of expected results: ", e);
						}
					}
				}
				continue;
			}
			for (int querySubstParam = 0; querySubstParam < subsParametersAmount; querySubstParam++) {
				ByteBuffer expectedResultsBuffer = ByteBuffer.wrap(tasks.get(taskId++).getExpectedAnswers());
				RabbitMQUtils.readString(expectedResultsBuffer);
				byte[] expectedResults = RabbitMQUtils.readString(expectedResultsBuffer).getBytes(StandardCharsets.UTF_8);
				try {
					FileUtils.writeByteArrayToFile(new File(resultsDir + File.separator + "versionigQuery" + (queryType + 1) + ".1." + (querySubstParam + 1) + "_results.json"), expectedResults);
				} catch (IOException e) {
					LOGGER.error("Exception caught during saving of expected results : ", e);
				}
				if (queryType == 0) break;
			}
		}
	}
	
	public void buildSPRQLTasks() {
		String queryString;
		String queriesPath = System.getProperty("user.dir") + File.separator + "queries";
		File queriesDir = new File(queriesPath);
		queriesDir.mkdirs();
		
		for (int queryType = 0, queryIndex = 0; queryType < Statistics.VERSIONING_QUERIES_COUNT; queryType++) {
			if (Arrays.asList(1,3,7).contains(queryType)) {
				for (int querySubType = 0; querySubType < Statistics.VERSIONING_SUB_QUERIES_COUNT; querySubType++) {	
					for (int querySubstParam = 0; querySubstParam < subsParametersAmount; querySubstParam++) {
						queryString = compileMustacheTemplate(queryType, queryIndex, querySubstParam);
						tasks.add(new Task((queryType + 1), Integer.toString(taskId++), queryString, null));
						try {
							FileUtils.writeStringToFile(new File(queriesDir + File.separator + "versionigQuery" + (queryType + 1) + "." + (querySubType + 1) + "." + (querySubstParam + 1) + ".sparql"), queryString);
						} catch (IOException e) {
							LOGGER.error("Exception caught during saving of generated task : ", e);
						}
					}
					queryIndex++;
				}
				continue;
			}
			for (int querySubstParam = 0; querySubstParam < subsParametersAmount; querySubstParam++) {
				queryString = compileMustacheTemplate(queryType, queryIndex, querySubstParam);
				tasks.add(new Task((queryType + 1), Integer.toString(taskId++), queryString, null));
				try {
					FileUtils.writeStringToFile(new File(queriesDir + File.separator + "versionigQuery" + (queryType + 1) + ".1." + (querySubstParam + 1) + ".sparql"), queryString);
				} catch (IOException e) {
					LOGGER.error("Exception caught during saving of generated task : ", e);
				}
				if (queryType == 0) break;
			}
			queryIndex++;
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
						cc = c.getConstructor(RandomUtil.class, HashMap.class, Definitions.class, String[].class);
						queryTemplate = (SubstitutionParametersGenerator) cc.newInstance(randomGenerator.randomUtilFactory(configuration.getLong(Configuration.GENERATOR_RANDOM_SEED)), versioningMustacheTemplatesHolder.getQueryTemplates(), definitions, null);					
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
		String entitiesFullPath = System.getProperty("user.dir") + File.separator + "entities.txt";
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
	    	for(int version=0; version<numberOfVersions; version++) {
	    		numberOfmessages.set(0);
	    		if(version == 0) {
	    			// ontologies have to be sent only from one data generator
	    			if(getGeneratorId() == 0) {
		    			// send ontology files to the system
		    			File ontologiesPathFile = new File(ontologiesPath);
		    			List<File> ontologiesFiles = (List<File>) FileUtils.listFiles(ontologiesPathFile, new String[] { "ttl" }, true);
		    			for (File file : ontologiesFiles) {
		    				String graphUri = "http://datagen.ontology." + file.getName();
		    				byte data[] = FileUtils.readFileToByteArray(file);
		    				byte[] dataForSending = RabbitMQUtils.writeByteArrays(null, new byte[][]{RabbitMQUtils.writeString(graphUri)}, data);
		    				sendDataToSystemAdapter(dataForSending);
		    				numberOfmessages.incrementAndGet();
		    			}
		    	    	LOGGER.info("All ontologies successfully sent to System Adapter.");
	    			}
	    	    	File dataPath = new File(initialVersionDataPath);
	    			List<File> dataFiles = (List<File>) FileUtils.listFiles(dataPath, new String[] { "added.nt" }, true);
	    			for (File file : dataFiles) {
	    				String graphUri = "http://datagen.version.0." + file.getName();
	    				byte data[] = FileUtils.readFileToByteArray(file);
	    				byte[] dataForSending = RabbitMQUtils.writeByteArrays(null, new byte[][]{RabbitMQUtils.writeString(graphUri)}, data);
	    				sendDataToSystemAdapter(dataForSending);
	    				numberOfmessages.incrementAndGet();
	    			}
	    		} else {
	    			File dataPath = new File(generatedDatasetPath + File.separator + "c" + version);
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
	    		}

    	    	LOGGER.info("Generated data for version " + version + " successfully sent to System Adapter.");
			
    	    	// TODO: sending of such signal when data were generated and not when data sent to
            	// sysada. a new signal has to be sent in such cases (do it when expected answers 
            	// computation removed from datagen implementation) now cannot to be done as 
            	// all data generated in the init function (before datagen's start)
            	LOGGER.info("Send signal to benchmark controller that all data (#" + numberOfmessages + ") of version " + version +" successfully sent to system adapter.");
            	ByteBuffer buffer = ByteBuffer.allocate(12);
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
	
	private int extractDeleted(String sourcePath, String cwIdsFile, String destPath) {
		int deletedTriples = 0;
		try {
			String scriptFilePath = System.getProperty("user.dir") + File.separator + "export_cws_tbd.sh";
			String[] command = {"/bin/bash", scriptFilePath, 
					sourcePath, cwIdsFile, destPath };
			Process p = new ProcessBuilder(command).start();
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ((line = in.readLine()) != null) {
				deletedTriples += Integer.parseInt(line);
			}
			p.waitFor();
		} catch (IOException e) {
            LOGGER.error("Exception while executing script for extracting creative works that have to be deleted.", e);
		} catch (InterruptedException e) {
            LOGGER.error("Exception while executing script for extracting creative works that have to be deleted.", e);
		}	
		return deletedTriples;
	}

	@Override
    public void receiveCommand(byte command, byte[] data) {
        if (command == VirtuosoSystemAdapterConstants.BULK_LOADING_DATA_FINISHED) {
        	versionLoadedFromSystemMutex.release();
        }
        super.receiveCommand(command, data);
    }
	
	@Override
	public void close() throws IOException {
		LOGGER.info("Closing Data Generator...");
		try {
			Thread.sleep(1000 * 60 * 60);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        super.close();
		LOGGER.info("Data Generator closed successfully.");
    }
}
