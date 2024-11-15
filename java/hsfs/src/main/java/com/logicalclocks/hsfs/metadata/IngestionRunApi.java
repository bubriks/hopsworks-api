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

package com.logicalclocks.hsfs.metadata;

import com.damnhandy.uri.template.UriTemplate;
import com.logicalclocks.hsfs.FeatureGroupBase;
import com.logicalclocks.hsfs.FeatureStoreException;
import com.logicalclocks.hsfs.IngestionRun;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class IngestionRunApi {

  public static final String FEATURE_GROUP_ID_PATH = "/featuregroups/{fgId}";
  public static final String FEATURE_GROUP_INGESTION_RUN = FEATURE_GROUP_ID_PATH
      + "/ingestionrun{?filter_by,sort_by,offset,limit}";

  private static final Logger LOGGER = LoggerFactory.getLogger(IngestionRunApi.class);

  public void saveIngestionRun(FeatureGroupBase featureGroupBase, IngestionRun ingestionRun)
      throws FeatureStoreException, IOException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_INGESTION_RUN;

    String uri = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", hopsworksClient.getProject().getProjectId())
        .set("fsId", featureGroupBase.getFeatureStore().getId())
        .set("fgId", featureGroupBase.getId())
        .expand();

    HttpPost postRequest = new HttpPost(uri);
    postRequest.setEntity(hopsworksClient.buildStringEntity(ingestionRun));
    hopsworksClient.handleRequest(postRequest);
  }

  public IngestionRun getIngestionRun(FeatureGroupBase featureGroupBase,  Map<String, String> queryParams)
      throws IOException, FeatureStoreException {
    HopsworksClient hopsworksClient = HopsworksClient.getInstance();
    String pathTemplate = HopsworksClient.PROJECT_PATH
        + FeatureStoreApi.FEATURE_STORE_PATH
        + FEATURE_GROUP_INGESTION_RUN;

    UriTemplate uriTemplate = UriTemplate.fromTemplate(pathTemplate)
        .set("projectId", hopsworksClient.getProject().getProjectId())
        .set("fsId", featureGroupBase.getFeatureStore().getId())
        .set("fgId", featureGroupBase.getId());
      
    if (queryParams != null) {
      for (Map.Entry<String, String> entry: queryParams.entrySet()) {
        uriTemplate.set(entry.getKey(), entry.getValue());
      }
    }

    String uri = uriTemplate.expand();

    LOGGER.info("Sending metadata request: " + uri);
    return hopsworksClient.handleRequest(new HttpGet(uri), IngestionRun.class);
  }
}
