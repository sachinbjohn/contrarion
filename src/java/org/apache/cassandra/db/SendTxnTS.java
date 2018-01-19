package org.apache.cassandra.db;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessageProducer;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.xerial.snappy.Snappy;

public class SendTxnTS implements MessageProducer {
    private final long transactionId;
    private final long lts;
    public static SendTxnTSSerializer serializer_ = new SendTxnTSSerializer();

    public static SendTxnTS fromBytes(byte[] raw, int version) throws IOException {
        return serializer_.deserialize(new DataInputStream(new FastByteArrayInputStream(raw)), version);
    }
    @Override
    public Message getMessage(Integer version) throws IOException {
        DataOutputBuffer dob = new DataOutputBuffer();
        serializer_.serialize(this, dob, version);

        byte[] msg = Snappy.compress(dob.getData());
        return new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.SEND_TXN_TS, Arrays.copyOf(msg, msg.length), version);
    }

    public long getLts() {
        return lts;
    }

    public long getTransactionId() {

        return transactionId;
    }

    public SendTxnTS(long transactionId, long lts) {
        this.transactionId = transactionId;
        this.lts = lts;
    }

    public static class SendTxnTSSerializer implements IVersionedSerializer<SendTxnTS> {
        @Override
        public void serialize(SendTxnTS sendTxnTS, DataOutput dos, int version) throws IOException {
            dos.writeLong(sendTxnTS.transactionId);
            dos.writeLong(sendTxnTS.lts);
        }

        @Override
        public SendTxnTS deserialize(DataInput dis, int version) throws IOException {
            long txnid = dis.readLong();
            long lts = dis.readLong();
            return new SendTxnTS(txnid, lts);
        }

        @Override
        public long serializedSize(SendTxnTS sendTxnTS, int version) {
            return 2*DBConstants.longSize;
        }
    }
}
