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

import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.AtomicKijiPutter;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiURI;

/**
 * Base class for Flume sinks that write to a Kiji table. Provides functionality common to sinks
 * that write to Kiji.
 */
public abstract class AbstractKijiSink
    extends AbstractSink
    implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractKijiSink.class);
  public static final String KIJI_URI_CONFIG_KEY = "tableuri";

  private KijiURI mUri;
  private KijiTable mTable;
  private AtomicKijiPutter mWriter;

  /**
   * Returns the URI of the Kiji table that this sink has been configured to write to.
   *
   * @return the URI of the Kiji table that this sink has been configured to write to.
   */
  protected KijiURI getUri() {
    return mUri;
  }

  /**
   * Returns a handle to the Kiji table this sink has been configured to write to.
   *
   * @return a handle to the Kiji table this sink has been configured to write to.
   */
  protected KijiTable getTable() {
    return mTable;
  }

  /**
   * Returns the KijiPutter this sink should use to perform puts.
   *
   * @return the KijiPutter this sink should use to perform puts.
   */
  protected AtomicKijiPutter getWriter() {
    return mWriter;
  }

  /** {@inheritDoc} */
  @Override
  public void configure(Context context) {
    final String uriString = Preconditions.checkNotNull(context.getString(KIJI_URI_CONFIG_KEY),
        "Must specify a target table URI in configuration.");
    mUri = KijiURI.newBuilder(uriString).build();

    // Ensure that the provided KijiURI contains the required fields.
    Preconditions.checkArgument(mUri.getInstance() != null,
        "%s does not specify an instance to write to.", mUri.toString());
    Preconditions.checkArgument(mUri.getTable() != null,
        "%s does not specify a table to write to.", mUri.toString());

    LOG.info("Configured to write to: {}", mUri.toString());
  }

  /** {@inheritDoc} */
  @Override
  public void start() {
    try {
      final Kiji kiji = Kiji.Factory.open(mUri);
      try {
        mTable = kiji.openTable(mUri.getTable());
        mWriter = mTable.getWriterFactory().openAtomicPutter();
      } finally {
        kiji.release();
      }
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
  }

  /** {@inheritDoc} */
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
