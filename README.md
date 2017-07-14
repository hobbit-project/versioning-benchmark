# Versioning benchmark

Versioning benchmark SPBv test the ability of versioning systems to efficiently manage evolving datasets and queries evaluated across the multiple versions of said datasets. It is based upon [LDBC’s Semantic Publishing Benchmark](https://github.com/ldbc/ldbc_spb_bm_2.0) for RDF database engines inspired by the Media/Publishing industry, particularly by the BBC’s Dynamic Semantic Publishing approach.

# Uploading Benchmark to HOBBIT Platform
Guidelines on how to upload a benchmark can be found here: https://github.com/hobbit-project/platform/wiki/Benchmark-your-system

# Running the Benchmark
If you want to run SPBv, please follow the guidelines found here: https://github.com/hobbit-project/platform/wiki/Experiments

**Description of SPBv parameters**:
* Generator seed: The random seed used for the data generator. The default value is 100.

* Generated data format: The erialization format for generated synthetic data. Available options are: TriG, TriX, N-Triples, N-Quads, N3, RDF/XML, RDF/JSON and Turtle. The default value is N-Triples.

* Seed year: Defines a seed year that will be used as starting point for generating the Creative Works date properties. The default value is set to 2016.

* Number of data generators: The number of Data Generators for this experiment. The default value is 1.

* Substitution parameters amount: The ammount of queries that will be produced for each query type. The default value is set to 2.

* Generation period in years: Defines the period in years of generated data, starting from ''seed year''. The default value is 1 
  
* Size of generated dataset: The size of the dataset that will be generated (including all versions) in terms of triples. The default value is 50000.

* Number of versions:  The number of different versions that will be produced. The default value is 12.
