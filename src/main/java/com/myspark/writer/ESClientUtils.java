package com.myspark.writer;

import com.myspark.properties.PropertiesReader;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ESClientUtils {

  private static Logger LOGGER = LoggerFactory.getLogger(ESClientUtils.class);
  private static volatile Object LOCK = new Object();
  private static RestHighLevelClient client;

  public static RestHighLevelClient getInstance() {
    if (client == null) {
      synchronized (LOCK) {
        if (client == null) {
          PropertiesReader propertiesReader = new PropertiesReader();
          Map esProperties = propertiesReader.readProperties("es_client.properties");
          String hostsString = (String) esProperties.get("es.hosts");
          LOGGER.info(" hosts : " + hostsString);

          String[] hostArr = hostsString.split(",");
          HttpHost[] hostNodes = new HttpHost[hostArr.length];

          for (int index = 0; index < hostArr.length; index++) {
            String host = hostArr[index];
            hostNodes[index] =
                new HttpHost(host.split(":")[0], Integer.parseInt(host.split(":")[1]));
          }

          RestClientBuilder restClientBuilder = RestClient.builder(hostNodes);
          // other properties can be set here
          client = new RestHighLevelClient(restClientBuilder);
        }
      }
    }
    return client;
  }
}
