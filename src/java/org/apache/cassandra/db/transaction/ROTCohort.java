package org.apache.cassandra.db.transaction;

import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class ROTCohort {
    private static Logger logger = LoggerFactory.getLogger(ROTCohort.class);
    public static ConcurrentHashMap<Long, ROT_Timestamp> map = new ConcurrentHashMap<>();
    public static void addTV(long id, long[] tv) {
        if(map.contains(id)) {
            ROT_Timestamp idts = map.get(id);
            idts.tv = tv;
            idts.cv.signal();
        } else {
            ROT_Timestamp new_idts = new ROT_Timestamp();
            ROT_Timestamp idts = map.putIfAbsent(id, new_idts);
            if(idts == null)
                idts = new_idts;
            idts.tv = tv;
            idts.cv.signal();
        }
    }
    public static long[] getTV(long id) throws InterruptedException {
        if(map.contains(id)) {
            ROT_Timestamp idts = map.get(id);
            idts.cv.await();
            map.remove(id);
            return idts.tv;
        } else {
            ROT_Timestamp new_idts = new ROT_Timestamp();
            ROT_Timestamp idts = map.putIfAbsent(id, new_idts);
            if(idts == null)
                idts = new_idts;
            idts.cv.await();
            map.remove(id);
            return idts.tv;
        }
    }
}
