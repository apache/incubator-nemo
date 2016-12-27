# Vortex 
[![Build Status](http://cmscluster.snu.ac.kr:8080/jenkins/buildStatus/icon?job=Vortex-master)](http://cmscluster.snu.ac.kr:8080/jenkins/job/Vortex-master/)

Vortex is a general-purpose data-processing engine.

## Components
* translator: Translates various high-level user programs for the compiler.
* compiler: Compiles user programs into a physical execution plan.
* runtime: Physically executes the plan.

## Requirements
* Beam 0.4.0-incubating-SNAPSHOT (You must download it from https://github.com/apache/incubator-beam and build it)
* Java 8
* Maven

## Examples
* java -cp target/vortex-0.1-SNAPSHOT-shaded.jar edu.snu.vortex.translator.beam.examples.MapReduce ./src/main/resources/sample_input ./src/main/resources/sample_output
* java -cp target/vortex-0.1-SNAPSHOT-shaded.jar edu.snu.vortex.translator.beam.examples.Broadcast ./src/main/resources/sample_input ./src/main/resources/sample_output
* java -cp target/vortex-0.1-SNAPSHOT-shaded.jar edu.snu.vortex.compiler.examples.MapReduce
