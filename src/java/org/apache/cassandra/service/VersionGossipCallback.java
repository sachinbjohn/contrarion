package org.apache.cassandra.service;

import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.ICompletable;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.ShortNodeId;


import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class VersionGossipCallback implements IAsyncCallback {
    long[][]VVs;
    int responses, expectedResponses;
    ICompletable completable;
    public VersionGossipCallback(long[][] VVs, ICompletable completable) {
        this.VVs = VVs;
        this.expectedResponses = VVs.length;
        responses = 1; //local vv already included
        this.completable = completable;
    }
    @Override
    public void response(Message msg) {
        try {
            long[] vv = new long[ShortNodeId.numDCs];
            VVs[responses++] = vv;
            ByteArrayInputStream inputByteStream = new ByteArrayInputStream(msg.getMessageBody());
            DataInputStream dis = new DataInputStream(inputByteStream);
            for (int i = 0; i < ShortNodeId.numDCs; ++i) {
                vv[i] = dis.readLong();
            }
            if(responses == expectedResponses)
                completable.complete();

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }

    @Override
    public boolean isLatencyForSnitch() {
        return false;
    }
}
