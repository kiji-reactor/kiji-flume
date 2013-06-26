package org.kiji.flume;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;

/**
 */
public class KijiSink extends AbstractSink implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(KijiSink.class);

  private KijiURI mUri;
  private KijiTable mTable;
  private KijiTableWriter mWriter;

  @Override
  public void configure(Context context) {
    // TODO: Add error message.
    final String uriString = Preconditions.checkNotNull(context.getString("tableuri"));
    mUri = KijiURI.newBuilder(uriString).build();
  }

  @Override
  public void start() {
    try {
      final Kiji kiji = Kiji.Factory.open(mUri);
      try {
        mTable = kiji.openTable(mUri.getTable());
        mWriter = mTable.openTableWriter();
      } finally {
        kiji.release();
      }
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
  }

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
      final EntityId entityId = mTable.getEntityId(tokens[0]);

      // Write the unpacked data to Kiji.
      mWriter.put(entityId, "info", "name", message);

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

  @Override
  public void stop () {
    try {
      mTable.release();
      mWriter.close();
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
  }
}
