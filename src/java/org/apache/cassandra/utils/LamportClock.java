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
 *
 */
public class LamportClock {
    //private static Logger logger = LoggerFactory.getLogger(LamportClock.class);

    //NO_CLOCK_TICK should only be used when calling function *locally* on the node
    public static final long NO_CLOCK_TICK = -1;

    //COPS_UNSUPPORTED should only be used in code that we don't intend to support like Hadoop on top of cassandra
    public static final long COPS_UNSUPPORTED = -2;

    private static AtomicLong logicalTime = new AtomicLong();
    private static Short localId = null;

    private LamportClock() {
        logicalTime.set(now());
    }

    //localId must be set before calling getVersion, otherwise you'll get a null exception

    /**
     * @return next "version" for this node, version is timestamp + nodeid
     */
    private static long now() {
        return System.currentTimeMillis();
    }

    public static long getVersion() {
        long localTime = logicalTime.incrementAndGet();
        long version = localTime; //(localTime << 16) + localId.shortValue();
        //logger.debug("getVersion {} = {} << 16 + {}", new Object[]{version, localTime, localId.shortValue()});
        return version;
    }

    //Should only be used for sanity checking
    public static long currentVersion() {
        throw new UnsupportedOperationException();
    }


    public static long sendTimestamp() {
        long newLocalTime = logicalTime.incrementAndGet();
        //logger.debug("sendTimestamp({})", newLocalTime);
        return newLocalTime;
    }

    public static long updateLocalTime(long lts) {
        long t = lts;
        long cur = logicalTime.get();
        long tnow = now();
        if (tnow > t)
            t = tnow;
        if (cur > t)
            t = cur;
        if (!logicalTime.compareAndSet(cur, t)) {
            do {
                cur = logicalTime.get();
                tnow = now();
                if (tnow > t)
                    t = tnow;
                if (cur > t)
                    t = cur;
            } while (!logicalTime.compareAndSet(cur, t));
        }
        return t;
    }

    public static long updateLocalTimeIncr(long lts) {
        long t = lts + 1;
        long cur = logicalTime.get() + 1;
        long tnow = now();
        if (tnow > t)
            t = tnow;
        if (cur > t)
            t = cur;
        if (!logicalTime.compareAndSet(cur, t)) {
            do {
                cur = logicalTime.get() + 1;
                tnow = now();
                if (tnow > t)
                    t = tnow;
                if (cur > t)
                    t = cur;
            } while (!logicalTime.compareAndSet(cur, t));
        }
        return t;
    }
    public static void setLocalTime(long lts) {
        logicalTime.set(lts);
    }

    public static synchronized void updateTime(long updateTime) {
        if (updateTime == NO_CLOCK_TICK) {
            //logger.debug("updateTimestamp(NO_CLOCK_TICK == {})", updateTime);
            return;
        }

        long localTime = logicalTime.longValue();
        long timeDiff = updateTime - localTime;

        long resultTime;
        if (timeDiff < 0) {
            resultTime = logicalTime.incrementAndGet();
        } else {
            resultTime = logicalTime.addAndGet(timeDiff+1);
        }
        //logger.debug("updateTimestamp({},{}) = {}", new Object[]{updateTime, localTime, resultTime});
    }

    public static void setLocalId(short localId2) {
        localId = localId2;
    }

    //HL: add a function to return current lts
    public static long getCurrentTime() {
        return logicalTime.longValue();
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

    public static long sendTranId() throws Exception {
        long localTime = logicalTime.get();
        long tranId = (localTime << 16) + parseToLong(InetAddress.getLocalHost().getHostAddress());
        return tranId;
    }
}
