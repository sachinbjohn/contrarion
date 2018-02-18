package org.apache.cassandra.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.LamportClock;

/**
 * The client context and dependency tracking is quite simple in COPS2 because
 * we only care about "nearest" dependencies.
 *
 * @author wlloyd
 *
 */
public class ClientContext {
    ArrayList<Long> DV;
    int xactIdGen;
    public ClientContext() {
        //Backward compatitbility reasons; Nothing to initialize

    }
    public ClientContext(int numDCs) {
        DV = new ArrayList<>(numDCs);
        xactIdGen = 0;
    }

    public void advanceDV(byte dcIndex, long lts) {
            DV.set(dcIndex, new Long(lts));
    }
    public void advanceDV(List<Long> otherDV) {
        for(int i  = 0; i < DV.size(); ++i) {
            if(otherDV.get(i) > DV.get(i))
                DV.set(i, otherDV.get(i));
        }
    }

    public long genXactId() throws  Exception {
        return LamportClock.sendTranId(++xactIdGen);
    }

    public final static int NOT_YET_SUPPORTED = -1;

    public HashSet<Dep> getDeps() {
        return null;
    }

    public void addDep(Dep dep) {
        //SBJ: Do nothing
    }

    public void addDeps(Set<Dep> deps) {
        //SBJ: Do nothing
    }

    public void addDep(int not_yet_supported) {
        //Place holder for parts I haven't figured out yet
        assert not_yet_supported == NOT_YET_SUPPORTED;
        assert false;
    }

    public void clearDeps() {  }

    public void addDep(ByteBuffer key, ColumnOrSuperColumn cosc)
    throws NotFoundException   {      }

    @Override
    public String toString() {
        return DV.toString();
    }
}
