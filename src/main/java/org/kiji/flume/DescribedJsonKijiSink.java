/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.flume;

import java.io.IOException;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.AtomicKijiPutter;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiIOException;

public class DescribedJsonKijiSink
    extends DescribedTextKijiSink {
  private static final Logger LOG = LoggerFactory.getLogger(DescribedJsonKijiSink.class);

  /**
   * Returns a string containing an element referenced by the specified path, or null if the
   * element isn't found.  This uses a period '.' delimited syntax similar to JSONPath
   * ({@linktourl http://goessner.net/articles/JsonPath/}).
   *
   * @param head JsonObject that is the head of the current JSON tree.
   * @param path delimited by periods
   * @return string denoting the element at the specified path.
   */
  private String getFromPath(JsonObject head, String path) {
    Preconditions.checkNotNull(head);
    Preconditions.checkNotNull(path);

    // Split the path into components using the delimiter for tree traversal.
    String[] pathComponents = path.split("\\.");

    // After getting the path components traverse the json tree.
    JsonElement jsonElement = head;
    for (String pathComponent : pathComponents) {
      if (jsonElement.isJsonObject()) {
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        if (jsonObject.has(pathComponent)) {
          jsonElement = jsonObject.get(pathComponent);
        } else {
          LOG.warn("Missing path component {} at current path {}.  Returning null.",
              pathComponent, jsonObject);
          return null;
        }
      }
    }
    if (jsonElement.isJsonPrimitive()) {
      return jsonElement.getAsString();
    }
    LOG.warn("Specified path {} is not complete for {}.  Returning null", path, head);
    return null;
  }

  /** {@inheritDoc} */
  @Override
  protected void buildPut(String text, AtomicKijiPutter putter) {
    JsonObject gson = new JsonParser().parse(text).getAsJsonObject();

    // Extract the EntityId for this line of JSON.
    final String entityIdSource = getFromPath(gson, getImportDescriptor().getEntityIdSource());
    if (entityIdSource == null) {
      LOG.warn("Unable to retrieve entityId from source field '{}' in '{}'",
          getImportDescriptor().getEntityIdSource(),
          text);
    } else {
      final EntityId entityId = getTable().getEntityId(entityIdSource);
      putter.begin(entityId);

      // Extract the timestamp for this line of JSON (if applicable).
      Long timestamp = null;
      if (null != getImportDescriptor().getOverrideTimestampSource()) {
        // Read the timestamp from the line of JSON.
        final String timestampSource =
            getFromPath(gson, getImportDescriptor().getOverrideTimestampSource());
        timestamp = Long.parseLong(timestampSource);
      }

      // Extract the values in this line of JSON.
      final Map<KijiColumnName, String> sourceMap = getImportDescriptor().getColumnNameSourceMap();
      for (KijiColumnName columnName : sourceMap.keySet()) {
        final String source = sourceMap.get(columnName);
        final String fieldValue = Preconditions.checkNotNull(getFromPath(gson, source),
            "Detected missing field: %s", source);

        try {
          if (null == timestamp) {
            putter.put(
                columnName.getFamily(),
                columnName.getQualifier(),
                fieldValue);
          } else {
            putter.put(
                columnName.getFamily(),
                columnName.getQualifier(),
                timestamp,
                fieldValue);
          }
        } catch (IOException e) {
          throw new KijiIOException(e);
        }
      }

      try {
        putter.commit();
      } catch (IOException ioe) {
        throw new KijiIOException(ioe);
      }
    }
  }
}
