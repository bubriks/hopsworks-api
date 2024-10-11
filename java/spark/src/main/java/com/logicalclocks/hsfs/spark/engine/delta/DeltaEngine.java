/*
 *  Copyright (c) 2024. Hopsworks AB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.logicalclocks.hsfs.spark.engine.delta;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logicalclocks.hsfs.spark.StreamFeatureGroup;
import com.logicalclocks.hsfs.spark.engine.KafkaDeserializer;

public class DeltaEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeltaEngine.class);

  public DeltaEngine() {
    
  }

  public void streamToDeltaTable(SparkSession sparkSession, StreamFeatureGroup streamFeatureGroup,
                                 Map<String, String> readOptions) throws Exception {

    Dataset<Row> kafkaDF = sparkSession
        .read()
        .format("KAFKA")
        .options(readOptions)
        .option("key.deserializer", StringDeserializer.class.getName())
        .option("value.deserializer", KafkaDeserializer.class.getName())
        .option(KafkaDeserializer.SUBJECT_ID, String.valueOf(streamFeatureGroup.getSubject().getId()))
        .option(KafkaDeserializer.FEATURE_GROUP_ID, String.valueOf(streamFeatureGroup.getId()))
        .option(KafkaDeserializer.FEATURE_GROUP_SCHEMA, streamFeatureGroup.getAvroSchema())
        .option(KafkaDeserializer.FEATURE_GROUP_ENCODED_SCHEMA, streamFeatureGroup.getEncodedAvroSchema())
        .option(KafkaDeserializer.FEATURE_GROUP_COMPLEX_FEATURES,
            new JSONArray(streamFeatureGroup.getComplexFeatures()).toString())
        .option(ConsumerConfig.GROUP_ID_CONFIG, String.valueOf(streamFeatureGroup.getId()))
        .option("startingOffsets", "earliest")  // Read from the earliest offset
        .option("endingOffsets", "latest")  // Read up to the latest offset
        .load();

    LOGGER.info(kafkaDF.head().json());

    //streamFeatureGroup.insert(kafkaDF);

    /*
    kafkaDF
        .writeStream()
        .format("delta")
        //.option("checkpointLocation", "/path/to/sparkCheckpoint")
        .start(streamFeatureGroup.getLocation());
     */
  }
}
