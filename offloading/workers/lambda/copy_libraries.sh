#!/usr/bin/env bash

cp $M2_HOME/org/apache/beam/beam-sdks-java-nexmark/2.6.0-SNAPSHOT/beam-sdks-java-nexmark-2.6.0-SNAPSHOT.jar jars/

cp $M2_HOME/org/apache/beam/beam-sdks-java-io-kafka/2.6.0-SNAPSHOT/beam-sdks-java-io-kafka-2.6.0-SNAPSHOT.jar jars/

#cp lib/* jars/
#cp kafka_lib/* jars/

cp ../../../compiler/frontend/beam/target/nemo-compiler-frontend-beam-0.2-SNAPSHOT.jar jars/
cp ../../../common/target/nemo-common-0.2-SNAPSHOT.jar jars/
cp ../common/target/offloading-worker-common-0.2-SNAPSHOT.jar jars/
cp ../../common/target/offloading-common-0.2-SNAPSHOT.jar jars/
cp ../../../runtime/lambda-executor/target/nemo-lambda-executor-0.2-SNAPSHOT.jar jars/
cp ../../../runtime/executor-common/target/executor-common-0.2-SNAPSHOT.jar jars/
cp ../../../runtime/message/target/nemo-runtime-message-0.2-SNAPSHOT.jar jars/
cp ../../../conf/target/nemo-conf-0.2-SNAPSHOT.jar jars/
