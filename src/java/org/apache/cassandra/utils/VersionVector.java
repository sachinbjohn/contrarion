package org.apache.cassandra.utils;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.db.VersionGossip;
import org.apache.cassandra.db.VersionGossipCompletion;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;

import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;

import org.apache.cassandra.service.VersionGossipCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.provider.SHA;

//Singleton class
public class VersionVector {
    private static Logger logger = LoggerFactory.getLogger(VersionVector.class);

    public static long[] VV = null;
    public static long[] GSV = null;

    public static void init(int numDCs, int maxServInDC) {
        if(VV != null) //Don't re-initialize.
            return;
        VV = new long[numDCs];
        GSV = new long[numDCs];
        Runnable doGossip = new Runnable() {
            @Override
            public void run() {
                try {

                    Message msg = new VersionGossip(GSV).getMessage(MessagingService.version_);
                    int numChilds = ShortNodeId.left != null ?  (ShortNodeId.right != null ? 2 : 1 ) : 0;
                    long[][] allVV = new long[numChilds+1][];
                    allVV[0] = VersionVector.VV;
                    VersionGossipCompletion completion =  new VersionGossipCompletion(allVV, null, null);

                    if (ShortNodeId.left != null) {
                        VersionGossipCallback callback = new VersionGossipCallback(allVV, completion);
                        MessagingService.instance().sendRR(msg, ShortNodeId.left, callback);
                        if (ShortNodeId.right != null) {
                            MessagingService.instance().sendRR(msg, ShortNodeId.right, callback);
                        }
                    } else {
                        completion.complete();
                    }
                } catch (IOException ex) {
                    logger.error("Error in sending VV", ex);
                }
            }
        };
        if (ShortNodeId.getNodeIdWithinDC(ShortNodeId.getLocalId()) == 0)
            StorageService.scheduledTasks.scheduleWithFixedDelay(doGossip, 1000, 10, TimeUnit.MILLISECONDS);
    }

    // public static long getLocalInGSV() {
    //     return GSV[ShortNodeId.getLocalDC()];
    // }
    // public static void setLocalInGSV(long lts) {
    //     GSV[ShortNodeId.getLocalDC()] =  lts;
    // }

    public static void updateVV(byte DC, long ut) {
        if(logger.isTraceEnabled())
            logger.trace("Update id {} local VV {} to {}", new Object[]{DC, VV, ut});
        if(VV[DC] < ut)
            VV[DC] = ut;
    }
    public static void updateGSVFromCoordinator(long[] TV) {
        int localDC = ShortNodeId.getLocalDC();
        for (int i = 0; i < GSV.length; ++i) {
            if (i != localDC && TV[i] > GSV[i])
                GSV[i] = TV[i];
        }
    }

    public static void updateGSVFromClient(List<Long> dvc) {
        int localDC = ShortNodeId.getLocalDC();
        for (int i = 0; i < GSV.length; ++i) {
            if (i != localDC && dvc.get(i) > GSV[i])
                GSV[i] = dvc.get(i);
        }
    }

}
