package org.apache.spark.shuffle.external;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.client.RpcResponseCallback;
import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.shuffle.protocol.UploadShufflePartitionStream;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;

public class ExternalShufflePartitionWriter implements ShufflePartitionWriter {

    private static final Logger logger =
        LoggerFactory.getLogger(ExternalShufflePartitionWriter.class);

    private final TransportClient client;
    private final String appId;
    private final String execId;
    private final int shuffleId;
    private final int mapId;
    private final int partitionId;

    private long totalLength = 0;
    private final ByteArrayOutputStream partitionBuffer = new ByteArrayOutputStream();

    public ExternalShufflePartitionWriter(
            TransportClient client,
            String appId,
            String execId,
            int shuffleId,
            int mapId,
            int partitionId) {
        this.client = client;
        this.appId = appId;
        this.execId = execId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
        this.partitionId = partitionId;
    }

    @Override
    public OutputStream openPartitionStream() {
        return partitionBuffer;
    }

    @Override
    public long commitAndGetTotalLength() {
        RpcResponseCallback callback = new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                logger.info("Successfully uploaded partition");
            }

            @Override
            public void onFailure(Throwable e) {
                logger.error("Encountered an error uploading partition", e);
            }
        };
        try {
            ByteBuffer streamHeader =
                new UploadShufflePartitionStream(
                        this.appId, execId, shuffleId, mapId,
                        partitionId).toByteBuffer();
            int size = partitionBuffer.size();
            byte[] buf = partitionBuffer.toByteArray();

            ManagedBuffer managedBuffer = new NioManagedBuffer(ByteBuffer.wrap(buf));
            client.uploadStream(new NioManagedBuffer(streamHeader), managedBuffer, callback);
            totalLength += size;
        } catch (Exception e) {
            client.close();
            logger.error("Encountered error while attempting to upload partition to ESS", e);
            throw new RuntimeException(e);
        } finally {
            client.close();
            logger.info("Successfully sent partition to ESS");
        }
        return totalLength;
    }

    @Override
    public void abort(Exception failureReason) {
        logger.error("Encountered error while attempting" +
            "to upload partition to ESS", failureReason);
    }
}
