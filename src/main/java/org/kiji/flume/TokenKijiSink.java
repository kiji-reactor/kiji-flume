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

import com.google.common.base.Preconditions;
import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;

/**
 */
public class TokenKijiSink
    extends AbstractKijiSink {
  private static final Logger LOG = LoggerFactory.getLogger(TokenKijiSink.class);

  @Override
  public Status process() throws EventDeliveryException {
    final Channel channel = getChannel();
    final Transaction transaction = channel.getTransaction();

    transaction.begin();
    try {
      // Unpack the incoming data.
      final Event event = channel.take();
      final String message = new String(event.getBody(), "UTF-8");

      LOG.info("Received message: {}", message);

      // Prepare the incoming data.
      final String[] tokens = message.split("\\s+");
      Preconditions.checkState(tokens.length > 1);
      final EntityId entityId = getTable().getEntityId(tokens[0]);

      getWriter().begin(entityId);
      // Write the unpacked data to Kiji.
      getWriter().put("info", "name", message);

      transaction.commit();
      getWriter().commit();
      return Status.READY;
    } catch (Throwable t) {
      transaction.rollback();
      getWriter().rollback();
      // re-throw all Errors
      if (t instanceof Error) {
        throw (Error)t;
      }
      return Status.BACKOFF;
    } finally {
      transaction.close();
    }
  }
}
