# Vortex 
[![Build Status](http://cmscluster.snu.ac.kr:8080/jenkins/buildStatus/icon?job=Vortex-master)](http://cmscluster.snu.ac.kr:8080/jenkins/job/Vortex-master/)

Vortex is a data-processing system composed of modular components.

## Components
* Compiler: Compiles a user program into an executable for the Engine.
* Engine: Physically executes the user program.

## Requirements
* Java 8
* Maven

## Examples
* `$ ./bin/run.sh -user_main edu.snu.vortex.examples.beam.MapReduce -optimization_policy none -user_args "./src/main/resources/sample_input_mr ./src/main/resources/sample_output"`
* `$ ./bin/run.sh -user_main edu.snu.vortex.examples.beam.MapReduce -optimization_policy pado -user_args "./src/main/resources/sample_input_mr ./src/main/resources/sample_output"`
* `$ ./bin/run.sh -user_main edu.snu.vortex.examples.beam.MapReduce -optimization_policy disaggregation -user_args "./src/main/resources/sample_input_mr ./src/main/resources/sample_output"`
* `$ ./bin/run.sh -user_main edu.snu.vortex.examples.beam.MapReduce -optimization_policy runtime_opt -user_args "./src/main/resources/sample_input_mr ./src/main/resources/sample_output"`
* `$ ./bin/run.sh -user_main edu.snu.vortex.examples.beam.Broadcast -optimization_policy pado -user_args "./src/main/resources/sample_input_mr ./src/main/resources/sample_output"`
* `$ ./bin/run.sh -user_main edu.snu.vortex.examples.beam.AlternatingLeastSquare -optimization_policy pado -user_args "./src/main/resources/sample_input_als 10 3"`
* `$ ./bin/run.sh -user_main edu.snu.vortex.examples.beam.MultinomialLogisticRegression -optimization_policy pado -user_args "./src/main/resources/sample_input_mlr 100 5 3"`
* `$ java -cp target/vortex-0.1-SNAPSHOT-shaded.jar edu.snu.vortex.compiler.optimizer.examples.MapReduce`

## DAG Visualization
Vortex Compiler and Engine stores JSON representation of intermediate DAGs.
`-dag_dir` option specifies the directory to store JSON files. By default JSON files are saved in `./target/dag`.

```bash
# Example for specifying target directory for JSON representation of DAGs.
./bin/run.sh -user_main edu.snu.vortex.examples.beam.AlternatingLeastSquare -optimization_policy pado -dag_dir "./target/dag/als" -user_args "./src/main/resources/sample_input_als 10 3"
```

You can easily visualize a DAG using [online visualizer](https://service.jangho.kr/vortex-dag/) with the corresponding JSON file.
