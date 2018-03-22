package org.apache.cassandra.db;

import org.apache.cassandra.db.transaction.ROTCohort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendTxnTSVerbHandler implements IVerbHandler {
    private static Logger logger = LoggerFactory.getLogger(SendTxnTSVerbHandler.class);

    @Override
    public void doVerb(Message message, String id) {
        SendTxnTS msg = null;
        try {
            msg = SendTxnTS.fromBytes(message.getMessageBody(), message.getVersion());
            ROTCohort.addTV(msg.getTransactionId(), msg.getTV());
        } catch (IOException e) {
            logger.error("Error decoding SendTxnMessage");
        }
    }
}
