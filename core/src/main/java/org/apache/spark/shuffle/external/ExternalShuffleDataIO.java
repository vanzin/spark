package org.apache.spark.shuffle.external;

import org.apache.spark.*;
import org.apache.spark.SecurityManager;
import org.apache.spark.internal.config.package$;
import org.apache.spark.network.TransportContext;
import org.apache.spark.network.netty.SparkTransportConf;
import org.apache.spark.network.server.NoOpRpcHandler;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.shuffle.api.ShuffleDataIO;
import org.apache.spark.shuffle.api.ShuffleReadSupport;
import org.apache.spark.shuffle.api.ShuffleWriteSupport;
import org.apache.spark.network.shuffle.k8s.KubernetesExternalShuffleClient;

import org.apache.spark.storage.BlockManager;
import scala.Tuple2;

import java.util.List;
import java.util.Random;

public class ExternalShuffleDataIO implements ShuffleDataIO {

    private final SparkConf conf;
    private final TransportConf transportConf;
    private final TransportContext context;
    private static MapOutputTracker mapOutputTracker;
    private static SecurityManager securityManager;
    private static List<Tuple2<String, Integer>> hostPorts;
    private static Boolean isDriver;
    private static KubernetesExternalShuffleClient shuffleClient;

    public ExternalShuffleDataIO(
            SparkConf sparkConf) {
        this.conf = sparkConf;
        // TODO: Grab numUsableCores
        this.transportConf = SparkTransportConf.fromSparkConf(sparkConf, "shuffle", 1);
        // Close idle connections
        this.context = new TransportContext(transportConf, new NoOpRpcHandler(), true, true);
    }

    @Override
    public void initialize() {
        SparkEnv env = SparkEnv.get();
        mapOutputTracker = env.mapOutputTracker();
        securityManager = env.securityManager();
        isDriver = env.blockManager().blockManagerId().isDriver();
        hostPorts = mapOutputTracker.getRemoteShuffleServiceAddress();
        if (isDriver) {
            shuffleClient = new KubernetesExternalShuffleClient(transportConf, securityManager,
            securityManager.isAuthenticationEnabled(), conf.getTimeAsMs(
                package$.MODULE$.SHUFFLE_REGISTRATION_TIMEOUT().key(), "5000ms"));
            shuffleClient.init(conf.getAppId());
            for (Tuple2<String, Integer> hp : hostPorts) {
                try {
                    shuffleClient.registerDriverWithShuffleService(
                        hp._1, hp._2,
                        conf.getTimeAsMs("spark.storage.blockManagerSlaveTimeoutMs",
                        conf.getTimeAsSeconds("spark.network.timeout", "120s") + "s"),
                        conf.getTimeAsSeconds(
                            package$.MODULE$.EXECUTOR_HEARTBEAT_INTERVAL().key(), "10s"));
                } catch (Exception e) {
                    throw new RuntimeException("Unable to register driver with ESS", e);
                }
            }
            BlockManager.ShuffleMetricsSource metricSource =
                new BlockManager.ShuffleMetricsSource(
                    "RemoteShuffleService", shuffleClient.shuffleMetrics());
            env.metricsSystem().registerSource(metricSource);
        }
    }

    @Override
    public ShuffleReadSupport readSupport() {
        return new ExternalShuffleReadSupport(
            transportConf, context, securityManager.isAuthenticationEnabled(), securityManager);
    }

    @Override
    public ShuffleWriteSupport writeSupport() {
        int rnd = new Random().nextInt(hostPorts.size());
        Tuple2<String, Integer> hostPort = hostPorts.get(rnd);
        return new ExternalShuffleWriteSupport(
            transportConf, context, securityManager.isAuthenticationEnabled(),
            securityManager, hostPort._1, hostPort._2);
    }

    @Override
    public void shuffleCleaned(int shuffleId) throws Exception {
      // TODO.
    }
}
