package org.kiji.flume;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.base.Preconditions;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.lib.bulkimport.KijiTableImportDescriptor;
import org.kiji.schema.AtomicKijiPutter;
import org.kiji.schema.KijiIOException;

/**
 * Flume sink that parses text data and writes to a Kiji table. This sink must be provided with a
 * {@link org.kiji.mapreduce.lib.bulkimport.KijiTableImportDescriptor} defining a mapping from
 * parsed text data to columns in a Kiji table.
 */
public abstract class DescribedTextKijiSink
    extends AbstractKijiSink {
  private static final Logger LOG = LoggerFactory.getLogger(DescribedTextKijiSink.class);
  public static final String IMPORT_DESC_CONFIG_KEY = "importdescriptor";

  private KijiTableImportDescriptor mImportDescriptor;

  /**
   * Override this method with code that takes an input text line and builds a put from it.
   *
   * @param text input to build a put from.
   * @param putter to put with.
   */
  protected abstract void buildPut(String text, AtomicKijiPutter putter);

  /**
   * Returns the import descriptor this sink has been configured to use.
   *
   * @return the import descriptor this sink has been configured to use.
   */
  protected KijiTableImportDescriptor getImportDescriptor() {
    return mImportDescriptor;
  }

  @Override
  public void configure(final Context context) {
    super.configure(context);

    // Load import descriptor.
    final String importDescriptorPath = Preconditions.checkNotNull(
        context.getString(IMPORT_DESC_CONFIG_KEY),
        "Must specify an import descriptor file in configuration");
    try {
      final InputStream inputStream = new FileInputStream(importDescriptorPath);
      try {
        mImportDescriptor = KijiTableImportDescriptor.createFromEffectiveJson(inputStream);
      } catch (IOException e) {
        LOG.error("Unable to load {}", importDescriptorPath);
        throw new KijiIOException(String.format("Unable to load %s", importDescriptorPath), e);
      } finally {
        inputStream.close();
      }
    } catch (IOException e) {
      LOG.error("Unable to load {}", importDescriptorPath);
      throw new KijiIOException(String.format("Unable to load %s", importDescriptorPath), e);
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
      final String text = new String(event.getBody(), "UTF-8");

      buildPut(text, getWriter());

      transaction.commit();

      return Status.READY;
    } catch (Throwable t) {
      // Rollback the in-progress put.
      transaction.rollback();
      try {
        getWriter().rollback();
      } catch (IllegalStateException ise) {
        if (!ise.getMessage().equals("rollback() must be paired with a call to begin()")) {
          throw ise;
        }
      }
      LOG.error(t.toString());

      // Re-throw all Errors.
      if (t instanceof Error) {
        throw (Error) t;
      }

      return Status.BACKOFF;
    } finally {
      transaction.close();
    }
  }
}
