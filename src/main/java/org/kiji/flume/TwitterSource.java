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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.scribe.builder.ServiceBuilder;
import org.scribe.builder.api.TwitterApi;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.oauth.OAuthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiIOException;

public class TwitterSource extends AbstractSource implements Configurable, PollableSource {
  private static final Logger LOG = LoggerFactory.getLogger(TwitterSource.class);
  public static final String API_KEY_PROPERTY = "apikey";
  public static final String API_SECRET_PROPERTY = "apisecret";
  public static final String TOKEN_PROPERTY = "token";
  public static final String TOKEN_SECRET = "tokensecret";
  public static final String TWITTER_URL = "https://stream.twitter.com/1.1/statuses/sample.json";

  private OAuthRequest mRequest;
  private BufferedReader mReader;

  @Override
  public void configure(Context context) {
    // Get the required secrets.
    final String apiKey = System.getProperty(API_KEY_PROPERTY);
    final String apiSecret = System.getProperty(API_SECRET_PROPERTY);
    final String accessToken = System.getProperty(TOKEN_PROPERTY);
    final String accessTokenSecret = System.getProperty(TOKEN_SECRET);
    LOG.info("API Key: {}", apiKey);
    LOG.info("API Secret: {}", apiSecret);
    LOG.info("Access Token: {}", accessToken);
    LOG.info("Access Token Secret: {}", accessTokenSecret);

    // Setup the connection with OAuth.
    final OAuthService service = new ServiceBuilder()
        .provider(TwitterApi.class)
        .apiKey(apiKey)
        .apiSecret(apiSecret)
        .build();
    final Token token = new Token(accessToken, accessTokenSecret);
    mRequest = new OAuthRequest(Verb.POST, TWITTER_URL);
    service.signRequest(token, mRequest);
  }

  @Override
  public void start() {
    // Open up a reader.
    final Response response = mRequest.send();
    mReader = new BufferedReader(new InputStreamReader(response.getStream()));
  }

  @Override
  public void stop() {
    try {
      mReader.close();
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
  }

  @Override
  public Status process() throws EventDeliveryException {
    final ChannelProcessor channelProcessor = getChannelProcessor();

    // Read the next line.
    String line;
    try {
      line = mReader.readLine();
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }

    // Process the line.
    if (line != null) {
      //LOG.info(line);

      final Event event = EventBuilder.withBody(line, Charset.forName("UTF-8"));
      channelProcessor.processEvent(event);

      return Status.READY;
    } else {
      return Status.BACKOFF;
    }
  }
}
