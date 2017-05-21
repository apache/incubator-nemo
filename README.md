# Vortex 
[![Build Status](http://cmscluster.snu.ac.kr:8080/jenkins/buildStatus/icon?job=Vortex-master)](http://cmscluster.snu.ac.kr:8080/jenkins/job/Vortex-master/)

Vortex is a data-processing system composed of modular components.

## Components
* Compiler: Compiles a user program into an executable for the Engine.
* Engine: Physically executes the user program.

## Requirements
* Java 8
* Maven
* Protobuf 3.2.0

## Examples
```bash
./bin/run.sh -job_id mr_none -user_main edu.snu.vortex.examples.beam.MapReduce -optimization_policy none -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output"
./bin/run.sh -job_id mr_pado -user_main edu.snu.vortex.examples.beam.MapReduce -optimization_policy pado -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output"
./bin/run.sh -job_id mr_disaggr -user_main edu.snu.vortex.examples.beam.MapReduce -optimization_policy disaggregation -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output"
./bin/run.sh -job_id mr_runtime_opt -user_main edu.snu.vortex.examples.beam.MapReduce -optimization_policy runtime_opt -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output"
./bin/run.sh -job_id broadcast_pado -user_main edu.snu.vortex.examples.beam.Broadcast -optimization_policy pado -user_args "`pwd`/src/main/resources/sample_input_mr `pwd`/src/main/resources/sample_output"
./bin/run.sh -job_id als_pado -user_main edu.snu.vortex.examples.beam.AlternatingLeastSquare -optimization_policy pado -user_args "`pwd`/src/main/resources/sample_input_als 10 3"
./bin/run.sh -job_id mlr_pado -user_main edu.snu.vortex.examples.beam.MultinomialLogisticRegression -optimization_policy pado -user_args "`pwd`/src/main/resources/sample_input_mlr 100 5 3"
java -cp target/vortex-0.1-SNAPSHOT-shaded.jar edu.snu.vortex.compiler.optimizer.examples.MapReduce
```

## DAG Visualization
Vortex Compiler and Engine stores JSON representation of intermediate DAGs.
`-dag_dir` option specifies the directory to store JSON files. By default JSON files are saved in `./target/dag`.

```bash
# Example for specifying target directory for JSON representation of DAGs.
./bin/run.sh -user_main edu.snu.vortex.examples.beam.AlternatingLeastSquare -optimization_policy pado -dag_dir "./target/dag/als" -user_args "`pwd`/src/main/resources/sample_input_als 10 3"
```

You can easily visualize a DAG using [online visualizer](https://service.jangho.kr/vortex-dag/) with the corresponding JSON file.

## Instructions for installing Protobuf
* Vortex uses v3.2.0 downloadable at: https://github.com/google/protobuf/releases/tag/v3.2.0
* Extract the downloaded tarball and command:
    - sudo ./configure
    - sudo make
    - sudo make check
    - sudo make install
* To check for a successful installation of v3.2.0: protoc --version
