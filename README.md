# Vortex 
[![Build Status](https://cmscluster.snu.ac.kr/jenkins/buildStatus/icon?job=Vortex-master)](https://cmscluster.snu.ac.kr/jenkins/job/Vortex-master/)

Vortex is a data-processing system composed of modular components.

## Components
* Compiler: Compiles a user program into an executable for the Engine.
* Engine: Physically executes the user program.

## Requirements
* Java 8
* Maven

## Examples
* ./bin/run.sh edu.snu.vortex.examples.beam.MapReduce none ./src/main/resources/sample_input_mr ./src/main/resources/sample_output
* ./bin/run.sh edu.snu.vortex.examples.beam.MapReduce pado ./src/main/resources/sample_input_mr ./src/main/resources/sample_output
* ./bin/run.sh edu.snu.vortex.examples.beam.MapReduce disaggregation ./src/main/resources/sample_input_mr ./src/main/resources/sample_output
* ./bin/run.sh edu.snu.vortex.examples.beam.MapReduce runtime_opt ./src/main/resources/sample_input_mr ./src/main/resources/sample_output
* ./bin/run.sh edu.snu.vortex.examples.beam.Broadcast pado ./src/main/resources/sample_input_mr ./src/main/resources/sample_output
* ./bin/run.sh edu.snu.vortex.examples.beam.AlternatingLeastSquare pado ./src/main/resources/sample_input_als 10 3
* java -cp target/vortex-0.1-SNAPSHOT-shaded.jar edu.snu.vortex.compiler.optimizer.examples.MapReduce
