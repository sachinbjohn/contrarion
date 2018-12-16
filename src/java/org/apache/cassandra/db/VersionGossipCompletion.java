package  org.apache.cassandra.db;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.ICompletable;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ShortNodeId;
import org.apache.cassandra.utils.VersionVector;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.provider.SHA;

public class VersionGossipCompletion implements ICompletable {
    private static Logger logger = LoggerFactory.getLogger(VersionGossipCompletion.class);

    volatile long[][] VVs;
    Message msgFromParent;
    String id;
    public VersionGossipCompletion(long[][] v, Message m, String id) {
        this.VVs = v;
        this.msgFromParent = m;
        this.id = id;
    }

    @Override
    public void complete() {
        try {
            if(msgFromParent != null) {
                DataOutputBuffer dob = new DataOutputBuffer(DBConstants.longSize * ShortNodeId.numDCs);

                for (int i = 0; i < ShortNodeId.numDCs; ++i) {
                    long min = VVs[0][i];
                    for (int node = 1; node < VVs.length; ++node) {
			 if (VVs[node][i] < min)
                            min = VVs[node][i];
                    }
                    dob.writeLong(min);
                }
                Message reply = msgFromParent.getReply(FBUtilities.getBroadcastAddress(), dob.getData(), msgFromParent.getVersion());
                MessagingService.instance().sendReply(reply, id, msgFromParent.getFrom());
            } else { //Root node
                for (int i = 0; i < ShortNodeId.numDCs; ++i) {
                    long min = VVs[0][i];
                    for (int node = 1; node < VVs.length; ++node) {
			if (VVs[node][i] < min)
                            min = VVs[node][i];
                    }
                    VersionVector.GSV[i] = min;
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
