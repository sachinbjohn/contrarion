package org.apache.cassandra.db;

import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;

import java.io.IOException;

import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.VersionGossipCallback;
import org.apache.cassandra.utils.FBUtilities;
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

            msg = VersionGossip.fromBytes(message.getMessageBody(), message.getVersion());
            VersionVector.updateGSVFromCoordinator(msg.getGSV());

            int numChilds = ShortNodeId.left != null ?  (ShortNodeId.right != null ? 2 : 1 ) : 0;
            Message fwd = new Message(FBUtilities.getBroadcastAddress(), StorageService.Verb.VERSION_GOSSIP, message.getMessageBody(), message.getVersion());
            long[][] allVV = new long[numChilds+1][];
            allVV[0] = VersionVector.VV;
            VersionGossipCompletion completion =  new VersionGossipCompletion(allVV, message, id);

            if (ShortNodeId.left != null) {
                VersionGossipCallback callback = new VersionGossipCallback(allVV, completion);
                MessagingService.instance().sendRR(fwd, ShortNodeId.left, callback);
                if (ShortNodeId.right != null) {
                    MessagingService.instance().sendRR(fwd, ShortNodeId.right, callback);
                }
            } else {
                completion.complete();
            }

        } catch (IOException e) {
            logger.error("Error decoding VersionGossipMessage");
        }
    }
}
