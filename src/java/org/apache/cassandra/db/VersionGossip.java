package org.apache.cassandra.db;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ShortNodeId;
import org.apache.cassandra.utils.VersionVector;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class VersionGossip implements MessageProducer {
    private final long[] VV;
    public static VersionGossipSerializer serializer_ = new VersionGossipSerializer();
    public long[] getVV() {
        return VV;
    }

    public VersionGossip(long[] VV) {
        this.VV = VV;
    }


    public static VersionGossip fromBytes(byte[] raw, int version) throws IOException {
        return serializer_.deserialize(new DataInputStream(new FastByteArrayInputStream(raw)), version);
    }
    @Override
    public Message getMessage(Integer version) throws IOException {
        DataOutputBuffer dob = new DataOutputBuffer();
        serializer_.serialize(this, dob, version);
        byte[] msg = dob.getData();
        return new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.VERSION_GOSSIP, Arrays.copyOf(msg, msg.length), version);
    }
    public static class VersionGossipSerializer implements IVersionedSerializer<VersionGossip> {
        @Override
        public void serialize(VersionGossip versionGossip, DataOutput dos, int version) throws IOException {
            for(int i = 0; i < ShortNodeId.numDCs; ++i) {
                dos.writeLong(VersionVector.VV[i]);
            }
        }

        @Override
        public VersionGossip deserialize(DataInput dis, int version) throws IOException {
            long[] vv = new long[ShortNodeId.numDCs];
            for(int i = 0; i < vv.length; ++i) {
                vv[i] = dis.readLong();
            }
            return new VersionGossip(vv);
        }

        @Override
        public long serializedSize(VersionGossip versionGossip, int version) {
            return DBConstants.longSize * ShortNodeId.numDCs;
        }
    }
}
