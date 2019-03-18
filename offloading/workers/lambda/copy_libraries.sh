#!/usr/bin/env bash

cp $M2_HOME/org/apache/beam/beam-sdks-java-nexmark/2.6.0-SNAPSHOT/beam-sdks-java-nexmark-2.6.0-SNAPSHOT.jar jars/
cp ../../../compiler/frontend/beam/target/nemo-compiler-frontend-beam-0.2-SNAPSHOT.jar jars/
cp ../../../common/target/nemo-common-0.2-SNAPSHOT-shaded.jar jars/
cp ../common/target/offloading-worker-common-0.2-SNAPSHOT.jar jars/
#cp ../../common/target/offloading-common-0.2-SNAPSHOT.jar jars/
cp ../../../runtime/lambda-executor/target/nemo-lambda-executor-0.2-SNAPSHOT.jar jars/
cp ../../../runtime/executor-common/target/executor-common-0.2-SNAPSHOT.jar jars/
#cp $M2_HOME/org/apache/beam/beam-sdks-java-io-hadoop-input-format/2.6.0/beam-sdks-java-io-hadoop-input-format-2.6.0.jar jars/
#cp $M2_HOME/org/apache/beam/beam-sdks-java-io-google-cloud-platform/2.6.0/beam-sdks-java-io-google-cloud-platform-2.6.0.jar jars/
