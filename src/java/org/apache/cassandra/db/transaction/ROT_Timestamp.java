package org.apache.cassandra.db.transaction;

import org.apache.cassandra.utils.SimpleCondition;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class ROT_Timestamp {
    long[] tv;
    SimpleCondition cv;
    ROT_Timestamp() {
        cv = new SimpleCondition();
    }
}


