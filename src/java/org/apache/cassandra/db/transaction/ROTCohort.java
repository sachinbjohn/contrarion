package org.apache.cassandra.db.transaction;

import java.util.concurrent.ConcurrentHashMap;

public class ROTCohort {
    public static ConcurrentHashMap<Long, ROT_Timestamp> map = new ConcurrentHashMap<>();
    public static void addTimestamp(long id, long lts) {
        if(map.contains(id)) {
            ROT_Timestamp idts = map.get(id);
            idts.lts = lts;
            idts.cv.signal();
        } else {
            ROT_Timestamp new_idts = new ROT_Timestamp();
            ROT_Timestamp idts = map.putIfAbsent(id, new_idts);
            idts.lts = lts;
            idts.cv.signal();
        }
    }
    public static long getTimestamp(long id) throws InterruptedException {
        if(map.contains(id)) {
            ROT_Timestamp idts = map.get(id);
            idts.cv.await();
            map.remove(id);
            return idts.lts;
        } else {
            ROT_Timestamp new_idts = new ROT_Timestamp();
            ROT_Timestamp idts = map.putIfAbsent(id, new_idts);
            idts.cv.await();
            map.remove(id);
            return idts.lts;
        }
    }
}
