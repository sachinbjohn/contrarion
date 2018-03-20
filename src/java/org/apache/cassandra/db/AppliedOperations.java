package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import org.apache.cassandra.net.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.xerial.snappy.Snappy;

public class AppliedOperations
{
     //WL TODO reduce the granularity of synchronization here

    public static synchronized void addPendingOp(ByteBuffer locatorKey, long timestamp)
    {
        throw new UnsupportedOperationException();
    }

    public static synchronized void addAppliedOp(ByteBuffer locatorKey, long timestamp)
    {
        throw new UnsupportedOperationException();
    }
}
