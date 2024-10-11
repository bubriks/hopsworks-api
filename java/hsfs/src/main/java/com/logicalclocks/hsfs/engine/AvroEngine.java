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

package com.logicalclocks.hsfs.engine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;

public class AvroEngine {

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroEngine.class);

  private final FeatureGroupUtils featureGroupUtils = new FeatureGroupUtils();

  private final BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(new byte[0], null);
  private final Schema schema; // schema of feature group
  private final Schema encodedSchema; // schema of feature group with complex features further serialized

  private final DatumReader<GenericRecord> encodedDatumReader;
  private final Map<String, DatumReader<GenericRecord>> complexFeaturesDatumReaders = new HashMap<>();


  public AvroEngine(FeatureGroupBase featureGroup) throws FeatureStoreException, IOException {
    this(featureGroup.getAvroSchema(), featureGroup.getEncodedAvroSchema(), featureGroup.getComplexFeatures());
  }

  public AvroEngine(String avroSchema, String encodedAvroSchema, Collection<String> complexFeatures)
      throws FeatureStoreException, IOException {
    GenericData.get().addLogicalTypeConversion(new Conversions.DecimalConversion());

    this.schema = new Schema.Parser().parse(avroSchema);
    this.encodedSchema = new Schema.Parser().parse(encodedAvroSchema);

    this.encodedDatumReader = new GenericDatumReader<>(this.encodedSchema);
    for (String feature: complexFeatures) {
      Schema featureSchema = new Schema.Parser().parse(featureGroupUtils.getFeatureAvroSchema(feature, schema));
      this.complexFeaturesDatumReaders.put(feature, new GenericDatumReader<>(featureSchema));
    }
  }

  // can't serialize since it is dependent on the engine
  //public byte[] serialize(Object data) {}

  public GenericRecord deserialize(byte[] data) {
    GenericRecord finalResult = new GenericData.Record(this.schema);
    GenericRecord result = null;

    try {
      Decoder decoder = DecoderFactory.get().binaryDecoder(data, binaryDecoder);
      result = encodedDatumReader.read(result, decoder);
    } catch (Exception ex) {
      LOGGER.error("Can't deserialize data '" + Arrays.toString(data) + "'", ex);
    }

    for (String feature : this.schema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList())) {
      if (this.complexFeaturesDatumReaders.containsKey(feature)) {
        // decode complex feature
        Object fieldObject = result.get(feature);
        try {
          Decoder decoder = DecoderFactory.get().binaryDecoder(((ByteBuffer) fieldObject).array(), binaryDecoder);
          finalResult.put(feature, complexFeaturesDatumReaders.get(feature).read(null, decoder));
        } catch (Exception ex) {
          LOGGER.info(
              "Can't deserialize complex feature data '" + fieldObject.toString() + "'", ex);
        }
      } else {
        finalResult.put(feature, result.get(feature));
      }
    }
    return finalResult;
  }
}
