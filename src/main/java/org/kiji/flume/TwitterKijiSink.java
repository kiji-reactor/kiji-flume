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

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;

public class TwitterKijiSink extends AbstractKijiSink {
  private static final Logger LOG = LoggerFactory.getLogger(TwitterKijiSink.class);
  private static final SimpleDateFormat DATE_PARSER =
      new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");

  @Override
  public Status process() throws EventDeliveryException {
    final Channel channel = getChannel();
    final Transaction transaction = channel.getTransaction();

    transaction.begin();
    try {
      final Event event = channel.take();
      final String message = new String(event.getBody(), "UTF-8");

      LOG.info("Received message: {}", message);

      final ObjectMapper oMapper = new ObjectMapper();
      final Map<String, Object> map =
          oMapper.readValue(message, new TypeReference<Map<String, Object>>(){});

      if (isTweet(map)) {
        final JsonNode node = oMapper.readTree(message);
        final JsonNode user = node.get("user");
        final EntityId eid = getTable().getEntityId(user.get("name").asText());
        final long timestamp  = DATE_PARSER
            .parse(node.get("created_at").asText())
            .getTime();

        final List<KijiColumnName> columnNames = getUri().getColumns();
        Preconditions.checkState(columnNames.size() == 1);
        Preconditions.checkState(columnNames.get(0).isFullyQualified());
        final String family = columnNames.get(0).getFamily();
        final String qualifier = columnNames.get(0).getQualifier();

        getWriter().begin(eid);
        getWriter().put(family, qualifier, timestamp, node.get("text").getValueAsText());
        getWriter().commit();
      }
      transaction.commit();
      return Status.READY;
    } catch (Throwable t) {
      LOG.error(t.toString());
      transaction.rollback();
      if (t instanceof Error) {
        throw (Error) t;
      }
      return Status.BACKOFF;
    } finally {
      transaction.close();
    }
  }

  private boolean isTweet(final Map<String, Object> map) {
    return map.containsKey("text") && map.containsKey("user");
  }
}
