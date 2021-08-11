# Nemo

[![Build Status](https://travis-ci.org/apache/incubator-nemo.svg?branch=master)](https://travis-ci.org/apache/incubator-nemo)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=org.apache.nemo%3Anemo-project&metric=alert_status)](https://sonarcloud.io/dashboard?id=org.apache.nemo%3Anemo-project)

A Data Processing System for Flexible Employment With Different Deployment Characteristics.

## Online Documentation

Details about Nemo and its development can be found in:
* Our website: https://nemo.apache.org/
* Our project wiki: https://cwiki.apache.org/confluence/display/NEMO/
* Our Dev mailing list for contributing: dev@nemo.apache.org [(subscribe)](mailto:dev-subscribe@nemo.apache.org)

Please refer to the [Contribution guideline](.github/CONTRIBUTING.md) to contribute to our project.

## Nemo prerequisites and setup

### Simple installation

Run `$ ./bin/install_nemo.sh` on the Nemo home directory. This script includes the actions described below.

### Prerequisites
* Java 8 or later (tested on Java 8 and Java 11)
* Maven
* YARN settings
    * Download Hadoop 2.7.2 at https://archive.apache.org/dist/hadoop/common/hadoop-2.7.2/
    * Set the shell profile as following:
        ```bash
        export HADOOP_HOME=/path/to/hadoop-2.7.2
        export YARN_HOME=$HADOOP_HOME
        export PATH=$PATH:$HADOOP_HOME/bin
        ```
* Protobuf 2.5.0
    * On Ubuntu 14.04 LTS and its point releases:

      ```bash
      $ sudo apt-get install protobuf-compiler
      ```

    * On Ubuntu 16.04 LTS and its point releases:

      ```bash
      $ sudo add-apt-repository ppa:snuspl/protobuf-250
      $ sudo apt update
      $ sudo apt install protobuf-compiler=2.5.0-9xenial1
      ```

    * On macOS:

      ```bash
      $ wget https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.bz2
      $ tar xvf protobuf-2.5.0.tar.bz2
      $ pushd protobuf-2.5.0
      $ ./configure CC=clang CXX=clang++ CXXFLAGS='-std=c++11 -stdlib=libc++ -O3 -g' LDFLAGS='-stdlib=libc++' LIBS="-lc++ -lc++abi"
      $ make -j 4
      $ sudo make install
      $ popd
      ```

    * Or build from source:

      * Downloadable at https://github.com/google/protobuf/releases/tag/v2.5.0
      * Extract the downloaded tarball
      * `$ ./configure`
      * `$ make`
      * `$ make check`
      * `$ sudo make install`

    *  To check for a successful installation of version 2.5.0, run `$ protoc --version`

### Installing Nemo
* Run all tests and install: `$ mvn clean install -T 2C`
* Run only unit tests and install: `$ mvn clean install -DskipITs -T 2C`

## Running Beam applications

Apache Nemo is an official runner of Apache Beam, and it can be executed from Beam, using NemoRunner, as well as directly from the Nemo project.
The details of using NemoRunner from Beam is shown on the [NemoRunner page of the Apache Beam website](https://beam.apache.org/documentation/runners/nemo/).
Below describes how Beam applications can be run directly on Nemo.

### Configurable options
* `-job_id`: ID of the Beam job
* `-user_main`: Canonical name of the Beam application
* `-user_args`: Arguments that the Beam application accepts
* `-optimization_policy`: Canonical name of the optimization policy to apply to a job DAG in Nemo Compiler
* `-deploy_mode`: `yarn` is supported(default value is `local`)

### Examples
```bash
## WordCount example from the Beam website (Count words from a document)
$ ./bin/run_beam.sh \
    -job_id beam_wordcount \
    -optimization_policy org.apache.nemo.compiler.optimizer.policy.DefaultPolicy \
    -user_main org.apache.nemo.examples.beam.BeamWordCount \
    -user_args "--runner=NemoRunner --inputFile=`pwd`/examples/resources/inputs/test_input_wordcount --output=`pwd`/outputs/wordcount"
$ less `pwd`/outputs/wordcount*

## MapReduce WordCount example (Count words from the Wikipedia dataset)
$ ./bin/run_beam.sh \
    -job_id mr_default \
    -executor_json `pwd`/examples/resources/executors/beam_test_executor_resources.json \
    -optimization_policy org.apache.nemo.compiler.optimizer.policy.DefaultPolicy \
    -user_main org.apache.nemo.examples.beam.WordCount \
    -user_args "`pwd`/examples/resources/inputs/test_input_wordcount `pwd`/outputs/wordcount"
$ less `pwd`/outputs/wordcount*

## YARN cluster example
$ ./bin/run_beam.sh \
    -deploy_mode yarn \
    -job_id mr_transient \
    -executor_json `pwd`/examples/resources/executors/beam_test_executor_resources.json \
    -user_main org.apache.nemo.examples.beam.WordCount \
    -optimization_policy org.apache.nemo.compiler.optimizer.policy.TransientResourcePolicy \
    -user_args "hdfs://v-m:9000/test_input_wordcount hdfs://v-m:9000/test_output_wordcount"

## NEXMark streaming Q0 (query0) example
$ ./bin/run_nexmark.sh \
    -job_id nexmark-Q0 \
    -executor_json `pwd`/examples/resources/executors/beam_test_executor_resources.json \
    -user_main org.apache.beam.sdk.nexmark.Main \
    -optimization_policy org.apache.nemo.compiler.optimizer.policy.StreamingPolicy \
    -scheduler_impl_class_name org.apache.nemo.runtime.master.scheduler.StreamingScheduler \
    -user_args "--runner=NemoRunner --streaming=true --query=0 --numEventGenerators=1 --manageResources=false --monitorJobs=false"

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
* `capacity`: Number of `Task`s that can be run in an executor. Set this value to be the same as the number of CPU cores of the container.

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

## Monitoring your job using Web UI
Please refer to the instructions at `web-ui/README.md` to run the frontend.

### Visualizing metric on run-time

While Nemo driver is alive, it can post runtime metrics through websocket. At your frontend, add websocket endpoint

```
ws://<DRIVER>:10101/api/websocket
```

where `<DRIVER>` is the hostname that Nemo driver runs.

OR, you can directly run the WebUI on the driver using `bin/run_webserver.sh`,
where it looks for the websocket on its local machine,
which, by default, provides the address at

```
http://<DRIVER>:3333
```

### Post-job analysis

On job completion, the Nemo driver creates `metric.json` at the directory specified by `-dag_dir` option. At your frontend, add the JSON file to do post-job analysis.

Other JSON files are for legacy Web UI, hosted [here](https:/nemo.snuspl.snu.ac.kr:50443/nemo-dag/). It uses [Graphviz](https://www.graphviz.org/) to visualize IR DAGs and execution plans.

### Examples
```bash
$ ./bin/run_beam.sh \
    -job_id als \
    -executor_json `pwd`/examples/resources/executors/beam_test_executor_resources.json \
    -user_main org.apache.nemo.examples.beam.AlternatingLeastSquare \
    -optimization_policy org.apache.nemo.compiler.optimizer.policy.TransientResourcePolicy \
    -dag_dir "./dag/als" \
    -user_args "`pwd`/examples/resources/inputs/test_input_als 10 3"
```

## Options for writing metric results to databases.

* `-db_enabled`: Whether or not to turn on the DB (`true` or `false`).
* `-db_address`: Address of the DB. (ex. PostgreSQL DB starts with `jdbc:postgresql://...`)
* `-db_id` : ID of the DB from the given address.
* `-db_password`: Credentials for the DB from the given address.

## Speeding up builds
* To exclude Spark related packages: mvn clean install -T 2C -DskipTests -pl \\!compiler/frontend/spark,\\!examples/spark
* To exclude Beam related packages: mvn clean install -T 2C -DskipTests -pl \\!compiler/frontend/beam,\\!examples/beam
* To exclude NEXMark related packages: mvn clean install -T 2C -DskipTests -pl \\!examples/nexmark
