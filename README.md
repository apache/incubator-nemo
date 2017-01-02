# Vortex 
[![Build Status](http://cmscluster.snu.ac.kr:8080/jenkins/buildStatus/icon?job=Vortex-master)](http://cmscluster.snu.ac.kr:8080/jenkins/job/Vortex-master/)

Vortex is a data-processing system composed of modular components.

## Components
* Compiler: Compiles a user program into a JobDAG for the Engine.
* Engine: Physically executes the JobDAG.

## Requirements
* Beam 0.4.0-incubating-SNAPSHOT (You must download it from https://github.com/apache/incubator-beam and build it)
* Java 8
* Maven

## Examples
* ./bin/run.sh edu.snu.vortex.examples.beam.MapReduce ./src/main/resources/sample_input ./src/main/resources/sample_output
* ./bin/run.sh edu.snu.vortex.examples.beam.Broadcast ./src/main/resources/sample_input ./src/main/resources/sample_output
* java -cp target/vortex-0.1-SNAPSHOT-shaded.jar edu.snu.vortex.compiler.examples.MapReduce
