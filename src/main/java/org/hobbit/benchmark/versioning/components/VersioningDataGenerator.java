package org.hobbit.benchmark.versioning.components;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
	private int datasetSizeInTriples;
	private int maxTriplesPerFile = 50000;
	private int dataGeneratorWorkers = 1;
	private int seedYear;
	private int generorPeriodYears;
	private int subGeneratorSeed;
	private int subsParametersAmount;
	private String generatedDatasetPath;
	private String serializationFormat;
	private int taskId = 0;
	
	private Configuration configuration = new Configuration();
	private Definitions definitions = new Definitions();
	private RandomUtil randomGenerator = null;
	
	private VersioningMustacheTemplatesHolder versioningMustacheTemplatesHolder = new VersioningMustacheTemplatesHolder();
	private SubstitutionQueryParametersManager substitutionQueryParamtersManager = new SubstitutionQueryParametersManager();
	
	private ArrayList<Task> tasks = new ArrayList<Task>();
	
	private int[] majorEvents;
	private int[] minorEvents;
	private int[] correlations;
		
	@Override
    public void init() throws Exception {
		LOGGER.info("Initializing Data Generator '" + getGeneratorId() + "'");
		super.init();
		
		String configurationFile = System.getProperty("user.dir") + File.separator + "test.properties";
		String definitionsFile = System.getProperty("user.dir") + File.separator + "definitions.properties";
		String dictionaryFile = System.getProperty("user.dir") + File.separator + "WordsDictionary.txt";
		
		configuration.loadFromFile(configurationFile); 
		definitions.loadFromFile(definitionsFile, configuration.getBoolean(Configuration.VERBOSE)); 
		
		// Initialize data generation parameters through the environment variables given by user
		initFromEnv();

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

		// Generate the data.
		LOGGER.info("Generating Creative Works data files...");
		long totalTriples = configuration.getLong(Configuration.DATASET_SIZE_TRIPLES);
		DataGenerator dataGenerator = new DataGenerator(randomGenerator, configuration, definitions, dataGeneratorWorkers, totalTriples, maxTriplesPerFile, generatedDatasetPath, serializationFormat);
		dataGenerator.produceData();

		LOGGER.info("Generating tasks...");
		// Generate the tasks.
		// 1) Generate tasks about ingestion speed
		for (int i = 0; i < numberOfVersions; i++) {
			tasks.add(new Task("1", Integer.toString(taskId++), "Version " + i + ", Ingestion task", null));
		}
		LOGGER.info("Ingestion tasks generated successfully.");

		// 2) Generate tasks about storage space
		tasks.add(new Task("2", Integer.toString(taskId++), "Storage space task", null));
		LOGGER.info("Storage space task generated successfully.");

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
		loadGeneratedData();

		// compute expected answers for all tasks
		LOGGER.info("Computing expected answers for generated SPRQL tasks...");
		computeExpectedAnswers();
		LOGGER.info("Expected answers have computed successfully for all generated SPRQL tasks.");	
		Thread.sleep(1000 * 60);
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
	
	public void computeExpectedAnswers() {	
		for (Task task : tasks) {
			String taskId = task.getTaskId();
			String taskQuery = task.getQuery();
			String taskType = task.getTaskType();
			
			long queryStart = 0;
			long queryEnd = 0;
			ResultSet results = null;
			
			byte[][] expectedAnswers = null;
					
			switch(Integer.parseInt(taskType)) {
			// ingestion task
			// compute the number of triples that expected to be loaded by the system.
			case 1:
				int version = Integer.parseInt(taskQuery.substring(8, taskQuery.indexOf(",")));
				String sparqlQueryString = ""
						+ "SELECT (COUNT(*) AS ?cnt) "
						+ "FROM <http://graph.version." + version + "> "
						+ "WHERE { ?s ?p ?o }";
				
				Query countQuery = QueryFactory.create(sparqlQueryString);
				QueryExecution cQexec = QueryExecutionFactory.sparqlService("http://localhost:8891/sparql", countQuery);
				queryStart = System.currentTimeMillis();
				results = cQexec.execSelect();
				queryEnd = System.currentTimeMillis();
				
				if(results.hasNext()) {
					String triplesToBeInserted = results.next().get("cnt").toString();
					expectedAnswers = new byte[1][];
					expectedAnswers[0] = RabbitMQUtils.writeString(triplesToBeInserted);
					task.setExpectedAnswers(RabbitMQUtils.writeByteArrays(expectedAnswers));
					tasks.set(Integer.parseInt(taskId), task);
					LOGGER.info("Ingestion task " + taskId + " triples : " + triplesToBeInserted);
				}
				break;
			// skip the storage space task
			case 2:
				continue;
			// query performance task
			case 3:
				// for query types query1 and query3, that refer to entire versions, we don't
				// evaluate the query due to extra time cost and expected answer length, but we 
				// only send the number of expected results
				if(taskQuery.contains("#  Query Name : query1") ||
						taskQuery.contains("#  Query Name : query3")) {
					taskQuery.replace("SELECT ?s ?p ?o", "SELECT (count(*) as ?cnt) ");
				}
				
				// execute the query on top of virtuoso to compute the expected answers
				Query query = QueryFactory.create(taskQuery);
				QueryExecution qexec = QueryExecutionFactory.sparqlService("http://localhost:8891/sparql", query);
				queryStart = System.currentTimeMillis();
				results = qexec.execSelect();
				queryEnd = System.currentTimeMillis();

				// track the number of expected answers, as long as the answers themselves
				expectedAnswers = new byte[2][];
				
				// update the task by setting its expected results
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
				ResultSetFormatter.outputAsJSON(outputStream, results);
				expectedAnswers[0] = RabbitMQUtils.writeString(Integer.toString(results.getRowNumber()));
				expectedAnswers[1] = outputStream.toByteArray();

				task.setExpectedAnswers(RabbitMQUtils.writeByteArrays(expectedAnswers));
				tasks.set(Integer.parseInt(taskId), task);
				break;
			}
						
			// TODO: for debugging purposes, delete it
			File expectedAnswersFile = new File(System.getProperty("user.dir") + File.separator + "expected_answers" + File.separator + "task" + taskId + "_results.json");
			FileOutputStream fos = null;
			try {
				fos = FileUtils.openOutputStream(expectedAnswersFile);
			} catch (IOException e) {
				LOGGER.error("Exception caught during the saving of task's: " + taskId + " expected answers: ", e);
			}
			
			ResultSetFormatter.outputAsJSON(fos, results) ;

			
			LOGGER.info("Expected answers for task " + taskId + " computed. Time : " + (queryEnd - queryStart) + " ms. Results num.: " + results.getRowNumber());
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
						tasks.add(new Task("3", Integer.toString(taskId++), queryString, null));
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
				tasks.add(new Task("3", Integer.toString(taskId++), queryString, null));
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
		int adjustedMajorEvents = (int) au.adjustAndGetMajorEventsAllocation(datasetSizeInTriples);
		int adjustedMinorEvents = (int) au.adjustAndGetMinorEventsAllocation(datasetSizeInTriples);
		int adjustedCorrelations= (int) au.adjustAndGetCorrelationsAllocation(datasetSizeInTriples);
		
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
		int triplesBasedOnDataGenerators = datasetSizeInTriples / numberOfGenerators;
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
		currDataGeneratorDatasetSizeInTriples = (approxGeneratedTriplesSoFar + currDataGeneratorDatasetSizeInTriples) >  datasetSizeInTriples ? datasetSizeInTriples - approxGeneratedTriplesSoFar : currDataGeneratorDatasetSizeInTriples;
		// if the number of generated triples so far has reached the target, let the current data generator 
		// to produce no triples			
		currDataGeneratorDatasetSizeInTriples = approxGeneratedTriplesSoFar > datasetSizeInTriples ? 0 : currDataGeneratorDatasetSizeInTriples;
		
		LOGGER.info("Generator '" + generatorId + "' will produce "+ (currDataGeneratorDatasetSizeInTriples > 0 ? "~" : "") 
				+ currDataGeneratorDatasetSizeInTriples + " triples,"
				+ " composed of:"
					+ "\n\t\t\t" + currDataGeneratorMajorEvents + " major events of total " + adjustedMajorEvents
					+ "\n\t\t\t" + currDataGeneratorMinorEvents + " minor events of total " + adjustedMinorEvents
					+ "\n\t\t\t" + currDataGeneratorCorrelations + " correlations of total " + adjustedCorrelations);

		// re-initialize test.properties file that is required for data generation
		configuration.setIntProperty("datasetSize", currDataGeneratorDatasetSizeInTriples);
		configuration.setIntProperty("numberOfVersions", numberOfVersions);
		configuration.setIntProperty("generatorRandomSeed", subGeneratorSeed);
		configuration.setIntProperty("hobbitDataGeneratorId", generatorId);
		configuration.setStringProperty("generateCreativeWorksFormat", serializationFormat);
		configuration.setStringProperty("creativeWorksPath", generatedDatasetPath);
		configuration.setStringProperty("generateCreativeWorksFormat", serializationFormat);
		configuration.setIntProperty("querySubstitutionParameters", subsParametersAmount);
	
		// re-initialize definitions.properties file that is required for data generation
		definitions.setIntProperty("seedYear", seedYear);
		definitions.setIntProperty("dataGenerationPeriodYears", generorPeriodYears);
		definitions.setIntProperty("majorEvents", currDataGeneratorMajorEvents);
		definitions.setIntProperty("minorEvents", currDataGeneratorMinorEvents);
		definitions.setIntProperty("correlationsAmount", currDataGeneratorCorrelations);
	}
	
	/**
     * A generic method for initialize benchmark parameters from environment variables
     * 
     * @param env
     *            a map of all available environment variables
     * @param parameter
     *            the property that we want to get
     * @param paramType
     *            a dummy parameter to recognize property's type
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
	
	public void initFromEnv() {
		LOGGER.info("Getting Data Generator's properites from the environment...");
		
		Map<String, String> env = System.getenv();
		datasetSizeInTriples = (Integer) getFromEnv(env, VersioningConstants.DATASET_SIZE_IN_TRIPLES, 0);
		numberOfVersions = (Integer) getFromEnv(env, VersioningConstants.NUMBER_OF_VERSIONS, 0);
		subGeneratorSeed = (Integer) getFromEnv(env, VersioningConstants.DATA_GENERATOR_SEED, 0) + getGeneratorId();
		seedYear = (Integer) getFromEnv(env, VersioningConstants.SEED_YEAR, 0);
		generorPeriodYears = (Integer) getFromEnv(env, VersioningConstants.GENERATION_PERIOD_IN_YEARS, 0);
		generatedDatasetPath = System.getProperty("user.dir") + File.separator + (String) getFromEnv(env, VersioningConstants.GENERATED_DATA_DIR, "");
		serializationFormat = (String) getFromEnv(env, VersioningConstants.GENERATED_DATA_FORMAT, "");
		subsParametersAmount = (Integer) getFromEnv(env, VersioningConstants.SUBSTITUTION_PARAMETERS_AMOUNT, 0);
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
		
		File ontologiesPath = new File("/versioning/ontologies");
		List<File> ontologiesFiles = (List<File>) FileUtils.listFiles(ontologiesPath, new String[] { "ttl" }, true);

		File dataPath = new File(generatedDatasetPath);
		String[] extensions = new String[] { RDFUtils.getFileExtensionFromRdfFormat(serializationFormat) };
		List<File> dataFiles = (List<File>) FileUtils.listFiles(dataPath, extensions, true);
		
		List<File> files = new ArrayList<File>(ontologiesFiles);
		files.addAll(dataFiles);
		
        try {
        	// send the ontologies to system adapter
        	// send generated data to system adapter
        	// all data have to be sent before sending the first query to system adapter
        	for (File file : files) {
        		byte[][] generatedFileArray = new byte[2][];
        		// send the file name and its content
        		generatedFileArray[0] = RabbitMQUtils.writeString(file.getAbsolutePath());
        		generatedFileArray[1] = FileUtils.readFileToByteArray(file);
        		// convert them to byte[]
        		byte[] generatedFile = RabbitMQUtils.writeByteArrays(generatedFileArray);
        		// send data to system
                sendDataToSystemAdapter(generatedFile);
    			LOGGER.info(file.getAbsolutePath() + " (" + (double) file.length() / 1000 + " KB) sent to System Adapter.");
        	}
        	LOGGER.info("All ontologies and generated data successfully sent to System Adapter.");
        	
        	// send generated tasks along with their expected answers to task generator
        	for (Task task : tasks) {
        		byte[] data = SerializationUtils.serialize(task);       			
    			sendDataToTaskGenerator(data);
            	LOGGER.info("Task " + task.getTaskId() + " sent to Task Generator.");
        	}
        	LOGGER.info("All generated tasks successfully sent to Task Generator.");
        } catch (Exception e) {
            LOGGER.error("Exception while sending file to System Adapter or Task Generator(s).", e);
        }
	}
	

	public void loadGeneratedData() {
		LOGGER.info("Loading generated data files to Virtuoso triplestore in order to compute gold standard...");

		try {
			String scriptFilePath = System.getProperty("user.dir") + File.separator + "load_to_virtuoso.sh";
			String[] command = {"/bin/bash", scriptFilePath, RDFUtils.getFileExtensionFromRdfFormat(serializationFormat), Integer.toString(numberOfVersions) };
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
	
	@Override
	public void close() throws IOException {
		LOGGER.info("Closing Data Generator...");
        super.close();
		LOGGER.info("Data Generator closed successfully.");
    }
}
