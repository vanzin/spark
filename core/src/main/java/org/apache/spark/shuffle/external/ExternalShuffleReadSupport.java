package org.apache.spark.shuffle.external;

import scala.compat.java8.OptionConverters;

import com.google.common.collect.Lists;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.crypto.AuthClientBootstrap;
import org.apache.spark.network.sasl.SecretKeyHolder;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.shuffle.api.ShufflePartitionReader;
import org.apache.spark.shuffle.api.ShuffleReadSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ExternalShuffleReadSupport implements ShuffleReadSupport {

    private static final Logger logger = LoggerFactory.getLogger(ExternalShuffleReadSupport.class);

    private final TransportConf conf;
    private final TransportContext context;
    private final boolean authEnabled;
    private final SecretKeyHolder secretKeyHolder;

    public ExternalShuffleReadSupport(
            TransportConf conf,
            TransportContext context,
            boolean authEnabled,
            SecretKeyHolder secretKeyHolder) {
        this.conf = conf;
        this.context = context;
        this.authEnabled = authEnabled;
        this.secretKeyHolder = secretKeyHolder;
    }

    @Override
    public ShufflePartitionReader newPartitionReader(String appId, int shuffleId, int mapId) {
        // TODO combine this into a function with ExternalShuffleWriteSupport
        List<TransportClientBootstrap> bootstraps = Lists.newArrayList();
        if (authEnabled) {
            bootstraps.add(new AuthClientBootstrap(conf, appId, secretKeyHolder));
        }
        TransportClientFactory clientFactory = context.createClientFactory(bootstraps);
        try {
            return new ExternalShufflePartitionReader(clientFactory,
                    appId,
                    shuffleId,
                    mapId);
        } catch (Exception e) {
            clientFactory.close();
            logger.error("Encountered creating transport client for partition reader");
            throw new RuntimeException(e); // what is standard practice here?
        }
    }
}
