hdfs dfs -rm -R /starlab

git pull origin starlab

mvn clean install

../spark-2.1.0-bin-hadoop2.7/bin/spark-submit --master yarn --conf "spark.executor.instances=12" --executor-memory 18g --num-executors 12 --class edu.snu.vortex.examples.beam.SparkMapReduce ./target/vortex-0.1-SNAPSHOT-shaded.jar rio-1:9092,rio-2:9092,rio-3:9092,rio-4:9092,rio-5:9092,rio-6:9092,rio-7:9092,rio-8:9092,rio-9:9092,rio-10:9092,rio-11:9092,rio-12:9092,rio-13:9092 starlab 5 5 5

#./bin/run.sh -args "edu.snu.vortex.examples.beam.CAYMapReduce!rio-1:9092,rio-2:9092,rio-3:9092,rio-4:9092,rio-5:9092,rio-6:9092,rio-7:9092,rio-8:9092,rio-9:9092,rio-10:9092,rio-11:9092,rio-12:9092,rio-13:9092!starlab!5" -eval_mem 22528 -eval_num 12 -runtime yarn
