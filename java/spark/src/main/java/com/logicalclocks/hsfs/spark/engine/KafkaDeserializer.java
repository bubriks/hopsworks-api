/*
 *  Copyright (c) 2021-2023. Hopsworks AB
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

package com.logicalclocks.hsfs.spark.engine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.logicalclocks.hsfs.engine.AvroEngine;

import lombok.SneakyThrows;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class KafkaDeserializer implements Deserializer<GenericRecord> {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDeserializer.class);

  public static final String SUBJECT_ID = "subjectId";
  public static final String FEATURE_GROUP_ID = "featureGroupId";
  public static final String FEATURE_GROUP_SCHEMA = "com.logicalclocks.hsfs.spark.StreamFeatureGroup.avroSchema";
  public static final String FEATURE_GROUP_ENCODED_SCHEMA =
      "com.logicalclocks.hsfs.spark.StreamFeatureGroup.encodedAvroSchema";
  public static final String FEATURE_GROUP_COMPLEX_FEATURES =
      "com.logicalclocks.hsfs.spark.StreamFeatureGroup.complexFeatures";

  private final ObjectMapper objectMapper = new ObjectMapper();

  private String subjectId;
  private String featureGroupId;
  private AvroEngine avroEngine;

  public KafkaDeserializer() {
  }

  @SneakyThrows
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.subjectId = (String) configs.get(SUBJECT_ID);
    this.featureGroupId = (String) configs.get(FEATURE_GROUP_ID);
    String featureGroupSchema = (String) configs.get(FEATURE_GROUP_SCHEMA);
    String encodedFeatureGroupSchema = (String) configs.get(FEATURE_GROUP_ENCODED_SCHEMA);
    String complexFeatureString = (String) configs.get(FEATURE_GROUP_COMPLEX_FEATURES);

    List<String> complexFeatures;
    try {
      String[] stringArray = objectMapper.readValue(complexFeatureString, String[].class);
      complexFeatures = Arrays.asList(stringArray);
    } catch (JsonProcessingException e) {
      throw new SerializationException("Could not deserialize complex feature array: " + complexFeatureString, e);
    }

    avroEngine = new AvroEngine(featureGroupSchema, encodedFeatureGroupSchema, complexFeatures);
  }

  @Override
  public GenericRecord deserialize(String topic, Headers headers, byte[] data) {
    if (subjectId.equals(getHeader(headers, "subjectId"))
        && featureGroupId.equals(getHeader(headers, "featureGroupId"))) {
      return deserialize(topic, data);
    }
    return null; // this job doesn't care about this entry, no point in deserializing
  }

  @Override
  public GenericRecord deserialize(String topic, byte[] data) {
    return avroEngine.deserialize(data);
  }

  private static String getHeader(Headers headers, String headerKey) {
    Header header = headers.lastHeader(headerKey);
    if (header != null) {
      return new String(header.value(), StandardCharsets.UTF_8);
    }
    return null;
  }

  @Override
  public void close() {

  }
}
