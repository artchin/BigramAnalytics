#!/bin/bash
# Bigram Document Frequency Pipeline
# Job 1: Extract bigrams and count documents
# Job 2: Sort by frequency and get Top-N

STREAMING_JAR="/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar"
INPUT_DATA="/data/wiki/en_articles" 
OUTPUT_JOB1="/user/$(whoami)/bigrams_counts"
OUTPUT_JOB2="/user/$(whoami)/bigrams_top10"

# Cleanup
hdfs dfs -rm -r -skipTrash ${OUTPUT_JOB1} 2>/dev/null
hdfs dfs -rm -r -skipTrash ${OUTPUT_JOB2} 2>/dev/null

# Job 1: Bigram extraction & document frequency
hadoop jar ${STREAMING_JAR} \
    -D mapreduce.job.name="Bigrams_Count" \
    -D mapreduce.job.reduces=8 \
    -input ${INPUT_DATA} \
    -output ${OUTPUT_JOB1} \
    -mapper mapper.py \
    -reducer reducer.py \
    -file mapper.py \
    -file reducer.py

# Job 2: Sort and get Top-10
hadoop jar ${STREAMING_JAR} \
    -D mapreduce.job.name="Bigrams_Top10" \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.partition.keycomparator.options="-k1,1nr" \
    -input ${OUTPUT_JOB1} \
    -output ${OUTPUT_JOB2} \
    -mapper mapper2.py \
    -reducer reducer2.py \
    -file mapper2.py \
    -file reducer2.py

# Results
echo "=== Top 10 Bigrams by Document Frequency ==="
hdfs dfs -cat ${OUTPUT_JOB2}/part-*
