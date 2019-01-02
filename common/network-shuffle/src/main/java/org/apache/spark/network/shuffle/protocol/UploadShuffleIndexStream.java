/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;
import org.apache.spark.network.protocol.Encoders;

// Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type;

/**
 * Upload shuffle index request to the External Shuffle Service.
 */
public class UploadShuffleIndexStream extends BlockTransferMessage {
    public final String appId;
    public final String execId;
    public final int shuffleId;
    public final int mapId;

    public UploadShuffleIndexStream(
            String appId,
            String execId,
            int shuffleId,
            int mapId) {
        this.appId = appId;
        this.execId = execId;
        this.shuffleId = shuffleId;
        this.mapId = mapId;
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof UploadShufflePartitionStream) {
            UploadShufflePartitionStream o = (UploadShufflePartitionStream) other;
            return Objects.equal(appId, o.appId)
                    && execId == o.execId
                    && shuffleId == o.shuffleId
                    && mapId == o.mapId;
        }
        return false;
    }

    @Override
    protected Type type() {
        return Type.UPLOAD_SHUFFLE_INDEX_STREAM;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(appId, execId, shuffleId, mapId);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("appId", appId)
                .add("execId", execId)
                .add("shuffleId", shuffleId)
                .add("mapId", mapId)
                .toString();
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId) +
                Encoders.Strings.encodedLength(execId) + 4 + 4;
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        Encoders.Strings.encode(buf, execId);
        buf.writeInt(shuffleId);
        buf.writeInt(mapId);
    }

    public static UploadShuffleIndexStream decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        int shuffleId = buf.readInt();
        int mapId = buf.readInt();
        return new UploadShuffleIndexStream(appId, execId, shuffleId, mapId);
    }
}
