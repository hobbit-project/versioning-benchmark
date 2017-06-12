/**
 * 
 */
package org.hobbit.benchmark.versioning.properties;

/**
 * @author papv
 *
 */
public final class VersioningConstants {

	// =============== COMMAND QUEUE CONSTANTS ===============

    public static final byte DATA_GEN_DATA_GENERATION_FINISHED = (byte) 301;

    public static final byte DATA_GEN_DATA_GENERATION_STARTED = (byte) 302;

    public static final byte VIRTUOSO_GS_READY_SIGNAL = (byte) 303;

	// =============== DATA GENERATOR CONSTANTS ===============

	public static final String DATA_GENERATOR_SEED = "data-generator_seed";

	public static final String DATA_GENERATOR_WORKERS = "data-generator_workers";

	public static final String NUMBER_OF_DATA_GENERATORS= "data-generators_number";
		
	// =============== TASK GENERATOR CONSTANTS ===============

	public static final String NUMBER_OF_TASK_GENERATORS= "task-generators_number";

	public static final String SUBSTITUTION_PARAMETERS_AMOUNT = "substitution-parameters_amount";

	// =============== DATA GENERATORS CONSTANTS ===============

	public static final String DATASET_SIZE_IN_TRIPLES = "dataset_size_in_triples";

	public static final String GENERATED_DATA_FORMAT = "generated_data_format";
	
	public static final String NUMBER_OF_VERSIONS = "number_of_versions";
		
	public static final String CREATIVE_WORKS_INFO = "creative_works_info";
	
	public static final String MAX_GENERATED_TRIPLES_PER_FILE = "max_generated_triples_per_file";
	
	public static final String SEED_YEAR = "seed_year";
	
	public static final String GENERATION_PERIOD_IN_YEARS = "generation_period_in_years";
	
	public static final String ARCHIVING_STRATEGY = "archiving_strategy";
	
	// =============== EVALUATION MODULE CONSTANTS ===============
	
	public static final String INITIAL_VERSION_INGESTION_SPEED = "initial-version_ingestion_speed";
	
	public static final String AVG_APPLIED_CHANGES_PS = "avg_applied_changes_ps";
	
	public static final String STORAGE_COST = "storage_cost";
	
	public static final String QT_1_AVG_EXEC_TIME = "query-type-1_avgerage_execution_time";

	public static final String QT_2_AVG_EXEC_TIME = "query-type-2_avgerage_execution_time";

	public static final String QT_3_AVG_EXEC_TIME = "query-type-3_avgerage_execution_time";

	public static final String QT_4_AVG_EXEC_TIME = "query-type-4_avgerage_execution_time";

	public static final String QT_5_AVG_EXEC_TIME = "query-type-5_avgerage_execution_time";

	public static final String QT_6_AVG_EXEC_TIME = "query-type-6_avgerage_execution_time";

	public static final String QT_7_AVG_EXEC_TIME = "query-type-7_avgerage_execution_time";

	public static final String QT_8_AVG_EXEC_TIME = "query-type-8_avgerage_execution_time";

	public static final String QUERY_FAILURES = "query-failures";

	public static final String INITIAL_VERSION_LOADING_TIME = "initial_version_loading-time";
	
//	public static final ArrayList<String> Ci_LOADING_TIME = "v0_loading-time";

	// =============== GOLD STANDARD CONSTANTS ===============
	
    public static final String DATA_GEN_DATA_2_GOLD_STD_QUEUE_NAME = "hobbit.datagen-data-gs";

    public static final String DATA_GEN_TASK_2_GOLD_STD_QUEUE_NAME = "hobbit.datagen-task-gs";

    public static final String GOLD_STD_2_DATA_GEN_QUEUE_NAME = "hobbit.gs-datagen";

    public static final String DATA_GEN_LOCALHOST = "hobbit.datagen-%d.localhost";


}
