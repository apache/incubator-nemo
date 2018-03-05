# Nemo

[![Build Status](https://travis-ci.org/apache/incubator-nemo.svg?branch=master)](https://travis-ci.org/apache/incubator-nemo)

A Data Processing System for Flexible Employment With Different Deployment Characteristics.

## Online Documentation

Details about Nemo and its development can be found in:
* Our website: https://nemo.apache.org/
* Our project wiki: https://cwiki.apache.org/confluence/display/NEMO/

## Nemo prerequisites and setup

### Prerequisites
* Java 8
* Maven
* YARN settings
    * Download Hadoop 2.7.4 at http://apache.tt.co.kr/hadoop/common/hadoop-2.7.4/
    * Set the shell profile as following:
        ```bash
        export HADOOP_HOME=/path/to/hadoop-2.7.4
        export YARN_HOME=$HADOOP_HOME
        export PATH=$PATH:$HADOOP_HOME/bin
        ```
* Protobuf 2.5.0
    * On Ubuntu 14.04 LTS and its point releases:

      ```bash
      sudo apt-get install protobuf-compiler
      ```

    * On Ubuntu 16.04 LTS and its point releases:

      ```bash
      sudo add-apt-repository ppa:snuspl/protobuf-250
      sudo apt update
      sudo apt install protobuf-compiler=2.5.0-9xenial1
      ```

    * On macOS:

      ```bash
      brew tap homebrew/versions
      brew install protobuf@2.5
      ```

    * Or build from source:

      * Downloadable at https://github.com/google/protobuf/releases/tag/v2.5.0
      * Extract the downloaded tarball
      * `./configure`
      * `make`
      * `make check`
      * `sudo make install`

    *  To check for a successful installation of version 2.5.0, run `protoc --version`

### Installing Nemo
* Run all tests and install: `mvn clean install -T 2C`
* Run only unit tests and install: `mvn clean install -DskipITs -T 2C`

## Running Beam applications
### Running an external Beam application
* Use run_external_app.sh instead of run.sh
* Set the first argument the path to the external Beam application jar

```bash
./bin/run_external_app.sh \
    `pwd`/nemo_app/target/bd17f-1.0-SNAPSHOT.jar \
    -job_id mapreduce \
    -executor_json `pwd`/examples/resources/sample_executor_resources.json \
    -user_main MapReduce \
    -user_args "`pwd`/mr_input_data `pwd`/nemo_output/output_data"
```

### Configurable options
* `-job_id`: ID of the Beam job
* `-user_main`: Canonical name of the Beam application
* `-user_args`: Arguments that the Beam application accepts
* `-optimization_policy`: Canonical name of the optimization policy to apply to a job DAG in Nemo Compiler
* `-deploy_mode`: `yarn` is supported(default value is `local`)

### Examples
```bash
## MapReduce example
./bin/run.sh \
	-job_id mr_default \
	-executor_json `pwd`/examples/resources/sample_executor_resources.json \
	-optimization_policy edu.snu.nemo.compiler.optimizer.policy.DefaultPolicy \
	-user_main edu.snu.nemo.examples.beam.MapReduce \
	-user_args "`pwd`/examples/resources/sample_input_mr `pwd`/examples/resources/sample_output_mr"

## YARN cluster example
./bin/run.sh \
	-deploy_mode yarn \
  	-job_id mr_pado \
	-executor_json `pwd`/examples/resources/sample_executor_resources.json \
  	-user_main edu.snu.nemo.examples.beam.MapReduce \
  	-optimization_policy edu.snu.nemo.compiler.optimizer.policy.PadoPolicy \
  	-user_args "hdfs://v-m:9000/sample_input_mr hdfs://v-m:9000/sample_output_mr"
```
## Resource Configuration
`-executor_json` command line option can be used to provide a path to the JSON file that describes resource configuration for executors. Its default value is `config/default.json`, which initializes one of each `Transient`, `Reserved`, and `Compute` executor, each of which has one core and 1024MB memory.

### Configurable options
* `num` (optional): Number of containers. Default value is 1
* `type`:  Three container types are supported:
  * `Transient` : Containers that store eviction-prone resources. When batch jobs use idle resources in `Transient` containers, they can be arbitrarily evicted when latency-critical jobs attempt to use the resources.
  * `Reserved` : Containers that store eviction-free resources. `Reserved` containers are used to reliably store intermediate data which have high eviction cost.
  * `Compute` : Containers that are mainly used for computation.
* `memory_mb`: Memory size in MB
* `capacity`: Number of `TaskGroup`s that can be run in an executor. Set this value to be the same as the number of CPU cores of the container.

### Examples
```json
[
  {
    "num": 12,
    "type": "Transient",
    "memory_mb": 1024,
    "capacity": 4
  },
  {
    "type": "Reserved",
    "memory_mb": 1024,
    "capacity": 2
  }
]
```

This example configuration specifies
* 12 transient containers with 4 cores and 1024MB memory each
* 1 reserved container with 2 cores and 1024MB memory

## Monitoring your job using web UI
Nemo Compiler and Engine can store JSON representation of intermediate DAGs.
* `-dag_dir` command line option is used to specify the directory where the JSON files are stored. The default directory is `./dag`.
  Using our [online visualizer](https://service.jangho.io/nemo-dag/), you can easily visualize a DAG. Just drop the JSON file of the DAG as an input to it.

### Examples
```bash
./bin/run.sh \
	-job_id als \
	-executor_json `pwd`/examples/resources/sample_executor_resources.json \
  	-user_main edu.snu.nemo.examples.beam.AlternatingLeastSquare \
  	-optimization_policy edu.snu.nemo.compiler.optimizer.policy.PadoPolicy \
  	-dag_dir "./dag/als" \
  	-user_args "`pwd`/examples/resources/sample_input_als 10 3"
```
