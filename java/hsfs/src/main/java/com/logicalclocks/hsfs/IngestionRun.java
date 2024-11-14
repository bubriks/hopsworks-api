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

package com.logicalclocks.hsfs;

import com.logicalclocks.hsfs.metadata.RestDto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
public class IngestionRun extends RestDto<IngestionRun> {

  @Getter
  @Setter
  private Integer id;

  @Getter
  @Setter
  private String startingOffsets;

  @Getter
  @Setter
  private String endingOffsets;

  @Getter
  private String currentOffsets;

  @Getter
  private Integer totalEntries;

  @Getter
  private Integer processedEntries;

  public IngestionRun(String startingOffsets, String endingOffsets) {
    this.startingOffsets = startingOffsets;
    this.endingOffsets = endingOffsets;
  }
  
}
