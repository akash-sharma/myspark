package com.myspark.config;

import com.myspark.properties.PropertiesReader;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ElasticConfig {

  private static Logger LOGGER = LoggerFactory.getLogger(ElasticConfig.class);

  private static RestHighLevelClient client;

  public static RestHighLevelClient getInstance() {
    try {
      if (client == null) {

        PropertiesReader propertiesReader = new PropertiesReader();
        Map esConfig = propertiesReader.readProperties("es_config.properties");

        String esHostsStr = (String) esConfig.get("hosts");
        LOGGER.info("ES hosts : {}", esHostsStr);

        String[] hostsArr = esHostsStr.split(",");
        HttpHost[] hostNodes = new HttpHost[hostsArr.length];

        for (int index = 0; index < hostsArr.length; index++) {
          String host = hostsArr[index];
          hostNodes[index] = new HttpHost(host.split(":")[0], Integer.parseInt(host.split(":")[1]));
        }

        client = new RestHighLevelClient(RestClient.builder(hostNodes));
        LOGGER.info("ES rest client created");
      }
    } catch (Exception e) {
      LOGGER.error("error in Elastic config ", e);
    }
    return client;
  }
}
