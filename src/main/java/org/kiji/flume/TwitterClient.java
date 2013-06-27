package org.kiji.flume;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.scribe.builder.ServiceBuilder;
import org.scribe.builder.api.TwitterApi;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.oauth.OAuthService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterClient {
  private static final Logger LOG = LoggerFactory.getLogger(TwitterClient.class);
  public static final String API_KEY_PROPERTY = "apikey";
  public static final String API_SECRET_PROPERTY = "apisecret";
  public static final String TOKEN_PROPERTY = "token";
  public static final String TOKEN_SECRET = "tokensecret";
  public static final String TWITTER_URL = "https://stream.twitter.com/1.1/statuses/sample.json";

  /**
   * Returns true if the JSON in line is a tweet as opposed to, for example, a delete or a flow
   * control message.
   *
   * @param line The JSON as a string.
   * @return true if the line represents a tweet.
   * @throws IOException If there's a problem parsing the JSON.
   */
  public static boolean isTweet(String line) throws IOException {
    Map<String, Object> map = new ObjectMapper()
        .readValue(line, new TypeReference<Map<String, Object>>(){});

    return map.containsKey("text") && map.containsKey("user");
  }

  public static void main(String[] args) throws IOException {
    final String apiKey = System.getProperty(API_KEY_PROPERTY);
    final String apiSecret = System.getProperty(API_SECRET_PROPERTY);
    final String accessToken = System.getProperty(TOKEN_PROPERTY);
    final String accessTokenSecret = System.getProperty(TOKEN_SECRET);

    final OAuthService service = new ServiceBuilder()
        .provider(TwitterApi.class)
        .apiKey(apiKey)
        .apiSecret(apiSecret)
        .build();
    final Token token = new Token(accessToken, accessTokenSecret);
    final OAuthRequest request = new OAuthRequest(Verb.POST, TWITTER_URL);
    service.signRequest(token, request);

    final Response response = request.send();
    final BufferedReader reader = new BufferedReader(new InputStreamReader(response.getStream()));

    String line = "";
    while ((line = reader.readLine()) != null) {
      if (isTweet(line)) {
        LOG.info(line);
      }
    }
  }
}
