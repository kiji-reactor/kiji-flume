package org.kiji.flume;

import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;

/**
 */
public abstract class AbstractKijiSink
    extends AbstractSink
    implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractKijiSink.class);
  private KijiURI mUri;
  private KijiTable mTable;
  private KijiTableWriter mWriter;

  protected KijiURI getUri() {
    return mUri;
  }

  protected KijiTable getTable() {
    return mTable;
  }

  protected KijiTableWriter getWriter() {
    return mWriter;
  }

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
  public void stop() {
    try {
      mTable.release();
      mWriter.close();
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
  }
}
