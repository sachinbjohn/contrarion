package org.apache.cassandra.service;

import org.apache.cassandra.net.IAsyncCallback;
import org.apache.cassandra.net.ICompletable;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.ShortNodeId;


import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class VersionGossipCallback implements IAsyncCallback {
    volatile long[][]VVs;
    int expectedResponses;
    AtomicInteger responses,finished;
    ICompletable completable;
    public VersionGossipCallback(long[][] VVs, ICompletable completable) {
        this.VVs = VVs;
        this.expectedResponses = VVs.length - 1;
        responses =  new AtomicInteger();  //0 is already filled with local
	finished = new AtomicInteger();
        this.completable = completable;
    }
    @Override
    public void response(Message msg) {
        try {
            long[] vv = new long[ShortNodeId.numDCs];
            ByteArrayInputStream inputByteStream = new ByteArrayInputStream(msg.getMessageBody());
            DataInputStream dis = new DataInputStream(inputByteStream);
            for (int i = 0; i < ShortNodeId.numDCs; ++i) {
                vv[i] = dis.readLong();
            }
            int pos = responses.incrementAndGet(); //0 is already filled with local
            VVs[pos] = vv;
	    int fin = finished.incrementAndGet();
            if (fin == expectedResponses)
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
