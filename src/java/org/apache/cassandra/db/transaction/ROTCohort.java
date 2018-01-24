package org.apache.cassandra.db.transaction;

import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class ROTCohort {
    private static Logger logger = LoggerFactory.getLogger(ROTCohort.class);
    public static ConcurrentHashMap<Long, ROT_Timestamp> map = new ConcurrentHashMap<>();
    public static void addTimestamp(long id, long lts) {
        if (logger.isTraceEnabled()) {
            logger.trace("Coordinator msg received. Adding lts = {} for xactId = {}", new Object[]{lts, id});
        }
        if(map.contains(id)) {
            ROT_Timestamp idts = map.get(id);
            idts.lts = lts;
            idts.cv.signal();
        } else {
            ROT_Timestamp new_idts = new ROT_Timestamp();
            ROT_Timestamp idts = map.putIfAbsent(id, new_idts);
            if(idts == null)
                idts = new_idts;
            idts.lts = lts;
            idts.cv.signal();
        }
    }
    public static long getTimestamp(long id) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Waiting for lts of xactId = {}", new Object[]{id});
        }
        if(map.contains(id)) {
            ROT_Timestamp idts = map.get(id);
            idts.cv.await();
            map.remove(id);
            if (logger.isTraceEnabled()) {
                logger.trace("Got lts = {} for xactId = {}", new Object[]{idts.lts, id});
            }
            return idts.lts;
        } else {
            ROT_Timestamp new_idts = new ROT_Timestamp();
            ROT_Timestamp idts = map.putIfAbsent(id, new_idts);
            if(idts == null)
                idts = new_idts;
            idts.cv.await();
            map.remove(id);
            if (logger.isTraceEnabled()) {
                logger.trace("Got lts = {} for xactId = {}", new Object[]{idts.lts, id});
            }
            return idts.lts;
        }
    }
}
