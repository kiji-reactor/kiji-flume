package org.kiji.flume;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiIOException;

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

      // Write the unpacked data to Kiji.
      getWriter().put(entityId, "info", "name", message);

      transaction.commit();
      return Status.READY;
    } catch (Throwable t) {
      transaction.rollback();

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
