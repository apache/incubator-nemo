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
* ./bin/run.sh edu.snu.vortex.examples.beam.MapReduce ./src/main/resources/sample_input_mr ./src/main/resources/sample_output
* ./bin/run.sh edu.snu.vortex.examples.beam.Broadcast ./src/main/resources/sample_input_mr ./src/main/resources/sample_output
* ./bin/run.sh edu.snu.vortex.examples.beam.AlternatingLeastSquare ./src/main/resources/sample_input_als 10 3
* java -cp target/vortex-0.1-SNAPSHOT-shaded.jar edu.snu.vortex.compiler.optimizer.examples.MapReduce
