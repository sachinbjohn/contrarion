package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.*;
import org.omg.CORBA.UNKNOWN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.ReadTransactionIdTracker;    //HL: for txnId check during dep_check
import org.apache.cassandra.db.DependencyCheck;
import java.io.IOException;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.db.DBConstants;
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

    public static synchronized void checkDependency(DependencyCheck depCheck, Message depCheckMessage, String id) {
        throw new UnsupportedOperationException();
    }
    public static void sendFetchTxnIdsReply(FetchTxnIds fetchId, Message fetchIdMessage, String id) {
        throw new UnsupportedOperationException();
    }
}
