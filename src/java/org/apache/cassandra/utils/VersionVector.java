package org.apache.cassandra.utils;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.db.VersionGossip;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.provider.SHA;

//Singleton class
public class VersionVector {
    private static Logger logger = LoggerFactory.getLogger(VersionVector.class);
    public static long[][] allVVs = null;
    public static long[] VV = null;
    public static long[] GSV = null;

    public static void init(int numDCs, int maxServInDC) {
        VV = new long[numDCs];
        allVVs = new long[maxServInDC][];
        int servNo = ShortNodeId.getNodeIdWithinDC(ShortNodeId.getLocalId());
        allVVs[servNo] = VV;
        GSV = new long[numDCs];
        Runnable doGossip = new Runnable() {
            @Override
            public void run() {
                try {

                    Message msg = new VersionGossip(VV).getMessage(MessagingService.version_);
                    for (InetAddress thisDCserver : ShortNodeId.getNonLocalAddressesInThisDC()) {
                        logger.error("Sending VV {} to {}", new Object[]{VV, thisDCserver});
                        MessagingService.instance().sendOneWay(msg, thisDCserver);
                    }
                } catch (IOException ex) {
                    logger.error("Error in sending VV", ex);
                }
            }
        };
        StorageService.scheduledTasks.scheduleWithFixedDelay(doGossip, 1000, 5, TimeUnit.MILLISECONDS);
    }

    // public static long getLocalInGSV() {
    //     return GSV[ShortNodeId.getLocalDC()];
    // }
    // public static void setLocalInGSV(long lts) {
    //     GSV[ShortNodeId.getLocalDC()] =  lts;
    // }
    public static void updateVVFromGossip(short nodeid, long[] vv) {
        logger.error("Recvd VV {} from node {}", new Object[]{vv, nodeid});
        allVVs[nodeid] = vv;
    }
    public static void updateVV(byte DC, long ut) {
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


    public static void updateGSV() {
        int numDCs = GSV.length;
        for (int i = 0; i < numDCs; ++i) {
            long min = VV[i];
            for (int node = 0; node < allVVs.length; ++node) {
                if (allVVs[node] != null && min > allVVs[node][i])
                    min = allVVs[node][i];
            }
            GSV[i] = min;
        }
        logger.error("Updating GSV to {}", new Object[]{GSV});
    }
}
