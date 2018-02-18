package org.apache.cassandra.db;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

import java.io.IOException;

import org.apache.cassandra.utils.ShortNodeId;
import org.apache.cassandra.utils.VersionVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class VersionGossipVerbHandler implements IVerbHandler {
    private static Logger logger = LoggerFactory.getLogger(VersionGossipVerbHandler.class);
    @Override
    public void doVerb(Message message, String id) {
        VersionGossip msg = null;
        try {
            short nodeid = ShortNodeId.getNodeIdWithinDC(ShortNodeId.getId(message.getFrom()));
            msg = VersionGossip.fromBytes(message.getMessageBody(), message.getVersion());
            VersionVector.updateVVFromGossip(nodeid, msg.getVV());
            VersionVector.updateGSV();
        } catch (IOException e) {
            logger.error("Error decoding VersionGossipMessage");
        }
    }
}
