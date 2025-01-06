/*
 *  Copyright (c) 2025. Hopsworks AB
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

package com.logicalclocks.hsfs;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
public class OnlineIngestion {

  @Getter
  @Setter
  private Integer id;

  @Getter
  @Setter
  private long numEntries;

  @Getter
  @Setter
  private String currentOffsets;

  @Getter
  @Setter
  private Integer processedEntries;

  @Getter
  @Setter
  private Integer insertedEntries;

  @Getter
  @Setter
  private Integer abortedEntries;

  @Getter
  @Setter
  private List<OnlineIngestionBatchResult> batchResults;

  @Getter
  @Setter
  private FeatureGroupBase featureGroup;

  public OnlineIngestion(long numEntries) {
    this.numEntries = numEntries;
  }

  public void waitForCompletion() {

  }

}