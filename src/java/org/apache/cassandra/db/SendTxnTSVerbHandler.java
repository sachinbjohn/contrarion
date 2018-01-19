package org.apache.cassandra.db;

import org.apache.cassandra.db.transaction.ROTCohort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendTxnTSVerbHandler implements IVerbHandler {
        private static Logger logger_ = LoggerFactory.getLogger(SendTxnTSVerbHandler.class);

        @Override
        public void doVerb(Message message, String id) {
            SendTxnTS msg = null;
            try {
                msg = SendTxnTS.fromBytes(message.getMessageBody(), message.getVersion());
                ROTCohort.addTimestamp(msg.getTransactionId(), msg.getLts());
            } catch (IOException e) {
                logger_.error("Error decoding SendTxnMessage");
            }
        }
}
