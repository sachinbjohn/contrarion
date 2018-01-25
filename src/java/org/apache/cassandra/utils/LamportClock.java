package org.apache.cassandra.utils;

import java.util.concurrent.atomic.AtomicLong;
import java.net.InetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * There should only be one LamportClock for each process, so this class is a
 * singleton (all static).
 *
 * @author wlloyd
 */
public class LamportClock {
    //private static Logger logger = LoggerFactory.getLogger(LamportClock.class);

    //NO_CLOCK_TICK should only be used when calling function *locally* on the node
    public static final long NO_CLOCK_TICK = -1;

    //COPS_UNSUPPORTED should only be used in code that we don't intend to support like Hadoop on top of cassandra
    public static final long COPS_UNSUPPORTED = -2;

    private static long logicalTime;
    private static Short localId = null;

    private LamportClock() {
        logicalTime = now();
    }

    //localId must be set before calling getVersion, otherwise you'll get a null exception

    /**
     * @return next "version" for this node, version is timestamp + nodeid
     */
    private static long now() {
        return System.currentTimeMillis();
    }

    public static synchronized long getVersion() {
        long localTime = ++logicalTime;
        long version = (localTime); // << 16) + localId.shortValue();
        //logger.debug("getVersion {} = {} << 16 + {}", new Object[]{version, localTime, localId.shortValue()});
        return version;
    }

    //Should only be used for sanity checking
    public static synchronized long currentVersion() {
        return (logicalTime);// << 16) + localId.shortValue();
    }


    public static synchronized long sendTimestamp() {
        long newLocalTime = ++logicalTime;
        //logger.debug("sendTimestamp({})", newLocalTime);
        return newLocalTime;
    }

    public static synchronized void updateTime(long updateTime) {
        if (updateTime == NO_CLOCK_TICK) {
            //logger.debug("updateTimestamp(NO_CLOCK_TICK == {})", updateTime);
            return;
        }

        long localTime = logicalTime;
        long timeDiff = updateTime - localTime;

        long resultTime;
        if (timeDiff < 0) {
            resultTime = ++logicalTime;
        } else {
            resultTime = logicalTime += (timeDiff + 1);
        }
        //logger.debug("updateTimestamp({},{}) = {}", new Object[]{updateTime, localTime, resultTime});
    }


    public static synchronized long updateLocalTime(long lts) {
        long tmax = lts;
        long cur = logicalTime;
        long tnow = now();
        if (tnow > tmax)
            tmax = tnow;
        if (cur > tmax)
            tmax = cur;
        logicalTime = tmax;
        return tmax;
    }

    public static synchronized long updateLocalTimeIncr(long lts) {
        long tmax = lts + 1;
        long cur = logicalTime + 1;
        long tnow = now();
        if (tnow > tmax)
            tmax = tnow;
        if (cur > tmax)
            tmax = cur;
        logicalTime = tmax;
        return tmax;
    }

    public static synchronized void setLocalTime(long lts) {
        if(lts > logicalTime)
            logicalTime = lts;
    }

    public static void setLocalId(short localId2) {
        localId = localId2;
    }

    //HL: add a function to return current lts
    public static synchronized long getCurrentTime() {
        return logicalTime;
    }

    //SNOW: for new way of generating txnid
    private static long parseToLong(String ipStr) throws Exception {
        if (ipStr.indexOf('.') < 0) {
            return 0L;
        }
        String ip = "";
        int index = 0;
        for (String str : ipStr.split("\\.")) {
            // we only use the last 16 bits of ip to identify each node
            if (++index <= 2)
                continue;
            ip = ip + str;
        }
        return Long.parseLong(ip);

    }

    public static synchronized long sendTranId() throws Exception {
        long localTime = logicalTime;
        long tranId = (localTime << 16) + parseToLong(InetAddress.getLocalHost().getHostAddress());
        return tranId;
    }
}