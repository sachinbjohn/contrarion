package org.apache.cassandra.utils;

import java.net.InetAddress;
import java.util.*;
import java.util.Map.Entry;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks the network topology determined by the property file snitch to
 * determine the short node id we can embed in versions (timestamps)
 * <p>
 * ShortNodeIds are 16 bits, high 4 bits identify the datacenter,
 * low 12 bits identify the node within that datacenter.
 * <p>
 * With this setup we can handle up to 8 datacenters with up to 4096 servers in each
 *
 * @author wlloyd
 */
public class ShortNodeId {
    private static Logger logger = LoggerFactory.getLogger(ShortNodeId.class);
    //TODO: Keep this consistent across node failures and additions
    private static Map<InetAddress, Short> addrToId = new HashMap<InetAddress, Short>();
    public static byte numDCs = 0;
    public static int maxServInDC = 0;
    public static InetAddress left = null, right = null;
    public static void updateShortNodeIds(Map<InetAddress, String[]> addrToDcAndRack) {
        first = true;
        synchronized (addrToId) {
            //Just doing the brain-deadest thing for now
            addrToId.clear();

            SortedMap<String, List<InetAddress>> datacenterToAddrs = new TreeMap<String, List<InetAddress>>();
            for (Entry<InetAddress, String[]> entry : addrToDcAndRack.entrySet()) {
                InetAddress addr = entry.getKey();
                String datacenter = entry.getValue()[0];

                if (!datacenterToAddrs.containsKey(datacenter)) {
                    datacenterToAddrs.put(datacenter, new ArrayList<InetAddress>());
                }
                datacenterToAddrs.get(datacenter).add(addr);
            }

            byte dcIndex = 0;
            for (List<InetAddress> addrs : datacenterToAddrs.values()) {
                short nodeIndex = 0;
                for (InetAddress addr : addrs) {
                    assert dcIndex < 8 && nodeIndex < 4096;
                    short fullIndex = (short) ((dcIndex << 12) + nodeIndex);
                    addrToId.put(addr, fullIndex);
                    nodeIndex++;
                }
                if(nodeIndex > maxServInDC)
                    maxServInDC = nodeIndex;
                dcIndex++;
            }
            numDCs = dcIndex;
            LamportClock.setLocalId(getLocalId());
        }
        int selfId = getNodeIdWithinDC(getLocalId());
        int leftId = selfId * 2 + 1;
        int rightId = selfId * 2 + 2;
        for(Map.Entry<InetAddress, Short> entry : addrToId.entrySet()) {
            int localId = getNodeIdWithinDC(entry.getValue());
            if(localId == leftId)
                left = entry.getKey();
            else if(localId == rightId)
                right = entry.getKey();
        }
        logger.debug("NodeIDs updated numDC = {}, maxNodes = {}", new Object[]{numDCs, maxServInDC});
        VersionVector.init(numDCs, maxServInDC);
    }

    public static short getId(InetAddress addr) {
        synchronized (addrToId) {
            assert addrToId.containsKey(addr) : "addr = " + addr + " not found in " + addrToId;
            return addrToId.get(addr).shortValue();
        }
    }

    public static byte getDC(InetAddress addr) {
        synchronized (addrToId) {
            assert addrToId.containsKey(addr) : "addr = " + addr + " not found in " + addrToId;
            return (byte) (addrToId.get(addr).shortValue() >> 12);
        }
    }

    public static short getNodeIdWithinDC(short ID) {
        return (short) (ID & ((1<<12)-1));
    }
    public static short getLocalId() {
        return getId(DatabaseDescriptor.getListenAddress());
    }

   static boolean first = true;
    public static Set<InetAddress> getNonLocalAddressesInThisDC() {
        Set<InetAddress> nonLocalAddresses = new HashSet<InetAddress>();
        byte localDC = getLocalDC();
        if(first)
            logger.debug("I am node {} of DC {} with address {}", new Object[]{getNodeIdWithinDC(getLocalId()), getLocalDC(), DatabaseDescriptor.getListenAddress()});
        for (InetAddress addr : addrToId.keySet()) {
            if (getDC(addr) == localDC && !addr.equals(DatabaseDescriptor.getListenAddress())) {
                nonLocalAddresses.add(addr);
            }
        }
        if(first)
            for(InetAddress add: nonLocalAddresses)
                logger.debug("Other nodes in this DC are node = {} dc={} add={}", new Object[]{getNodeIdWithinDC(getId(add)),getDC(add), add});
        first = false;
        return nonLocalAddresses;
    }

    public static byte getLocalDC() {
        return getDC(DatabaseDescriptor.getListenAddress());
    }

}
