/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.thrift;

import java.io.IOError;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import com.google.common.collect.ArrayListMultimap;
import org.antlr.runtime.RecognitionException;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql.CQLStatement;
import org.apache.cassandra.cql.QueryProcessor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.migration.*;
import org.apache.cassandra.db.transaction.BatchMutateTransactionCohort;
import org.apache.cassandra.db.transaction.BatchMutateTransactionCoordinator;
import org.apache.cassandra.db.transaction.ROTCohort;
import org.apache.cassandra.db.transaction.TransactionProxy;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.FastByteArrayOutputStream;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.scheduler.IRequestScheduler;
import org.apache.cassandra.service.*;
import org.apache.cassandra.thrift.ThriftConverter.ChosenColumnResult;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.ColumnOrSuperColumnHelper.EvtAndLvt;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;

public class CassandraServer implements Cassandra.Iface
{
    private static Logger logger = LoggerFactory.getLogger(CassandraServer.class);

    private final static int COUNT_PAGE_SIZE = 1024;
    private static Collection<IColumn> emptyCols = Collections.emptyList();

    // thread local state containing session information
    public final ThreadLocal<ClientState> clientState = new ThreadLocal<ClientState>()
    {
        @Override
        public ClientState initialValue()
        {
            return new ClientState();
        }
    };

    /*
     * RequestScheduler to perform the scheduling of incoming requests
     */
    private final IRequestScheduler requestScheduler;

    public CassandraServer()
    {
        requestScheduler = DatabaseDescriptor.getRequestScheduler();
    }

    public ClientState state()
    {
        SocketAddress remoteSocket = SocketSessionManagementService.remoteSocket.get();
        ClientState retval = null;
        if (null != remoteSocket)
        {
            retval = SocketSessionManagementService.instance.get(remoteSocket);
            if (null == retval)
            {
                retval = new ClientState();
                SocketSessionManagementService.instance.put(remoteSocket, retval);
            }
        }
        else
        {
            retval = clientState.get();
        }
        return retval;
    }

    protected Map<DecoratedKey, ColumnFamily> readColumnFamily(List<ReadCommand> commands, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        // TODO - Support multiple column families per row, right now row only contains 1 column family
        Map<DecoratedKey, ColumnFamily> columnFamilyKeyMap = new HashMap<DecoratedKey, ColumnFamily>();

        List<Row> rows;
        try
        {
            schedule(DatabaseDescriptor.getRpcTimeout());
            try
            {
                rows = StorageProxy.read(commands, consistency_level);
            }
            finally
            {
                release();
            }
        }
        catch (TimeoutException e)
        {
            logger.debug("... timed out");
        	throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        for (Row row: rows)
        {
            columnFamilyKeyMap.put(row.key, row.cf);
        }
        return columnFamilyKeyMap;
    }


    private interface ISliceMap
    {
    }

    private class ThriftifiedSliceMap implements ISliceMap
    {
        public final Map<ByteBuffer, List<ColumnOrSuperColumn>> thriftifiedMap;

        public ThriftifiedSliceMap(Map<ByteBuffer, List<ColumnOrSuperColumn>> thriftifiedMap)
        {
            this.thriftifiedMap = thriftifiedMap;
        }
    }

    private class InternalSliceMap implements ISliceMap
    {
        public final Map<ByteBuffer, Collection<IColumn>> cassandraMap;

        public InternalSliceMap(Map<ByteBuffer, Collection<IColumn>> cassandraMap)
        {
            this.cassandraMap = cassandraMap;
        }
    }

    private ISliceMap getSlice(List<ReadCommand> commands, ConsistencyLevel consistency_level, boolean thriftify)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        Map<DecoratedKey, ColumnFamily> columnFamilies = readColumnFamily(commands, consistency_level);
        if (thriftify) {
            Map<ByteBuffer, List<ColumnOrSuperColumn>> thriftifiedColumnFamiliesMap = new HashMap<ByteBuffer, List<ColumnOrSuperColumn>>();
            for (ReadCommand command: commands)
            {
                ColumnFamily cf = columnFamilies.get(StorageService.getPartitioner().decorateKey(command.key));
                boolean reverseOrder = command instanceof SliceFromReadCommand && ((SliceFromReadCommand)command).reversed;
                List<ColumnOrSuperColumn> thriftifiedColumns = ThriftConverter.thriftifyColumnFamily(cf, command.queryPath.superColumnName != null, reverseOrder);
                thriftifiedColumnFamiliesMap.put(command.key, thriftifiedColumns);
            }
            return new ThriftifiedSliceMap(thriftifiedColumnFamiliesMap);
        } else {
            Map<ByteBuffer, Collection<IColumn>> columnFamiliesMap = new HashMap<ByteBuffer, Collection<IColumn>>();
            for (ReadCommand command: commands)
            {
                ColumnFamily cf = columnFamilies.get(StorageService.getPartitioner().decorateKey(command.key));
                if (cf == null) {
                    columnFamiliesMap.put(command.key, emptyCols);
                    try {
                        if(logger.isTraceEnabled())
                            logger.trace("Missing key " + ByteBufferUtil.string(command.key));
                    } catch (Exception ex) {}
                }
                else
                    columnFamiliesMap.put(command.key, cf.getSortedColumns());
            }
            return new InternalSliceMap(columnFamiliesMap);
        }
    }

    @Override
    public GetSliceResult get_slice(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        LamportClock.updateTime(lts);
        logger.debug("get_slice");

        state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
        List<ColumnOrSuperColumn> result = multigetSliceInternal(state().getKeyspace(), Collections.singletonList(key), column_parent, predicate, consistency_level).get(key);
        if (logger.isTraceEnabled()) {
            logger.trace("get_slice({}, {}, {}, {}, {}) = {}", new Object[]{ByteBufferUtil.bytesToHex(key), column_parent, predicate, consistency_level, lts, result});
        }
        return new GetSliceResult(result, LamportClock.sendTimestamp());
    }

    private void sendTxnTs(String keyspace, List<ByteBuffer> remoteKeys, long txnid, long[] tv) {
        for (ByteBuffer key : remoteKeys) {
            List<InetAddress> localEndpoints = StorageService.instance.getLocalLiveNaturalEndpoints(keyspace, key);
            assert localEndpoints.size() == 1 : "Assumed for now";
            InetAddress localEndpoint = localEndpoints.get(0);
            try {
                // logger.error("Sending TV for id "+txnid+ " to "+localEndpoint);
                Message msg = new SendTxnTS(txnid, tv).getMessage(Gossiper.instance.getVersion(localEndpoint));
                MessagingService.instance().sendOneWay(msg, localEndpoint);
            } catch (IOException ex) {
                throw new IOError(ex);
            }
        }
    }

    @Override
    public MultigetSliceResult rot_coordinator(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level, long transactionId, List<ByteBuffer> remoteKeys, List<Long> dvc)
            throws InvalidRequestException, UnavailableException, TimedOutException {
        try {
            VersionVector.updateGSVFromClient(dvc);
            int localDCid = ShortNodeId.getLocalDC();
            long localTime = LamportClock.updateLocalTime(dvc.get(localDCid));
            VersionVector.GSV[localDCid] = localTime;
            long[] chosenTime = VersionVector.GSV.clone();

            if(logger.isTraceEnabled()) {
                String keyStr = "";
                for (ByteBuffer key : keys)
                    keyStr += ByteBufferUtil.string(key) + ";";
                logger.trace("Transaction {} :: coordinator size={} key={} lts = {} chosenTime = {}", new Object[]{transactionId, keys.size(), keyStr, localTime, chosenTime});
            }

            String keyspace = state().getKeyspace();
            sendTxnTs(keyspace, remoteKeys, transactionId, chosenTime); //send txn vector to cohorts

            state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
            ISliceMap iSliceMap = multigetSliceInternal(keyspace, keys, column_parent, predicate, consistency_level, false);
            assert iSliceMap instanceof InternalSliceMap : "thriftified was false, so it should be an internal map";
            Map<ByteBuffer, Collection<IColumn>> keyToColumnFamily = ((InternalSliceMap) iSliceMap).cassandraMap;
            //select results for each key that were visible at the chosen_time
            Map<ByteBuffer, List<ColumnOrSuperColumn>> keyToChosenColumns = new HashMap<ByteBuffer, List<ColumnOrSuperColumn>>();
            Set<Long> pendingTransactionIds = new HashSet<Long>();  //SBJ: Dummy
            //pendingTransactions for now is always null -- we don't consider WOT for now
            selectChosenResults(keyToColumnFamily, predicate, chosenTime, keyToChosenColumns);

            // for(Entry<ByteBuffer, List<ColumnOrSuperColumn>> entry : keyToChosenColumns.entrySet()) {
            //     logger.error("ROT Coordinator lts = {} chosenTime = {} now = {} logicalTime = {} Key = {} COSC = {}", new Object[]{lts, chosenTime, System.currentTimeMillis(), LamportClock.getCurrentTime(), ByteBufferUtil.string(entry.getKey()), entry.getValue()});
            // }
            chosenTime[localDCid] = LamportClock.getCurrentTime();
            List<Long> chosenTimeAsList = LongStream.of(chosenTime).boxed().collect(Collectors.toList());
            MultigetSliceResult result = new MultigetSliceResult(keyToChosenColumns, chosenTimeAsList);
            return result;

        } catch (Exception ex) {
            logger.error("ROT coordinator has error", ex);
            throw new InvalidRequestException(ex.getLocalizedMessage());
        }
    }

    @Override
    public MultigetSliceResultCohort rot_cohort(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level, long transactionId, List<Long> dvc)
            throws InvalidRequestException, UnavailableException, TimedOutException {

        try {
            String keyStr = "";
            if (logger.isTraceEnabled()) {
                for (ByteBuffer key : keys)
                    keyStr += ByteBufferUtil.string(key) + ";";
                logger.trace("Transaction {} ::  cohort size={}  key={} lts={}", new Object[]{transactionId, keys.size(), keyStr, dvc});
            }

            state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
            ISliceMap iSliceMap = multigetSliceInternal(state().getKeyspace(), keys, column_parent, predicate, consistency_level, false);
            assert iSliceMap instanceof InternalSliceMap : "thriftified was false, so it should be an internal map";
            Map<ByteBuffer, Collection<IColumn>> keyToColumnFamily = ((InternalSliceMap) iSliceMap).cassandraMap;


            //Wait until it receives timestamp from coordinator
            long[] chosenTime = ROTCohort.getTV(transactionId);
            int localDCid = ShortNodeId.getLocalDC();
            long lamport = LamportClock.updateLocalTime(chosenTime[localDCid]);
            VersionVector.updateGSVFromCoordinator(chosenTime);
            if (logger.isTraceEnabled()) {
                logger.trace("Transaction {} ::  cohort chosen time ={}", new Object[]{transactionId, chosenTime});
            }

            //select results for each key that were visible at the chosen_time
            Map<ByteBuffer, List<ColumnOrSuperColumn>> keyToChosenColumns = new HashMap<ByteBuffer, List<ColumnOrSuperColumn>>();
            //pendingTransactions for now is always null -- we don't consider WOT for now
            selectChosenResults(keyToColumnFamily, predicate, chosenTime, keyToChosenColumns);

            // for(Entry<ByteBuffer, List<ColumnOrSuperColumn>> entry : keyToChosenColumns.entrySet()) {
            //     logger.error("ROT Cohort lts = {} chosenTime = {}  now = {} logicalTime = {} Key = {} COSC = {}", new Object[]{lts, chosenTime, System.currentTimeMillis(), LamportClock.getCurrentTime(),  ByteBufferUtil.string(entry.getKey()), entry.getValue()});
            // }

            MultigetSliceResultCohort result = new MultigetSliceResultCohort(keyToChosenColumns, LamportClock.getCurrentTime()); //SBJ Need to current timestamp after read
            return result;

        } catch (Exception ex) {
            String keyStr = null;
            try {  keyStr = ByteBufferUtil.string(keys.get(0)); } catch (Exception e) {   }
            logger.error("ROT Cohort has error for key" + keyStr, ex);
            throw new InvalidRequestException(ex.getLocalizedMessage());
        }
    }


    @Override
    /*
     * HL: multiget_slice is called by clientlibrary's transactional_mutliget_slice
     * We put our main algr' logic here.
     * Check ReadTranctionIdTracker to see if other parts of this txn took place
     * If yes, then return old value by calling multiget_slice_by_time
     * If no, then put current txn's id into tracker and call multiget_slice
     */
    public MultigetSliceResult multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level, long lts)
            throws InvalidRequestException, UnavailableException, TimedOutException {

        throw new UnsupportedOperationException();
    }


    private void selectChosenResults(Map<ByteBuffer, Collection<IColumn>> keyToColumnFamily, SlicePredicate predicate, long[] chosen_time, Map<ByteBuffer, List<ColumnOrSuperColumn>> keyToChosenColumns)
    {
        for(Entry<ByteBuffer, Collection<IColumn>> entry : keyToColumnFamily.entrySet()) {
            ByteBuffer key = entry.getKey();
            Collection<IColumn> columns = entry.getValue();

            List<ColumnOrSuperColumn> chosenColumns = new ArrayList<ColumnOrSuperColumn>();
            for (IColumn column : columns) {
                ChosenColumnResult ccr = ThriftConverter.selectChosenColumn(column, chosen_time, key);
                if (ccr.pendingTransaction) {
                    throw new IllegalStateException("Pending transaction");
                } else {
                    chosenColumns.add(ccr.cosc);
                }
            }

            if (predicate.isSetSlice_range() && predicate.slice_range.reversed) {
                Collections.reverse(chosenColumns);
            }
            keyToChosenColumns.put(key, chosenColumns);
        }
    }

    @Override
    public MultigetSliceResult multiget_slice_by_time(List<ByteBuffer> keys, ColumnParent column_parent,
    SlicePredicate predicate, ConsistencyLevel consistency_level, long chosen_time, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
      throw new UnsupportedOperationException();
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> multigetSliceInternal(String keyspace, List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        ISliceMap iSliceMap = multigetSliceInternal(keyspace, keys, column_parent, predicate, consistency_level, true);
        assert iSliceMap instanceof ThriftifiedSliceMap : "thriftied was true, so this should be a thrifitied map";
        return ((ThriftifiedSliceMap) iSliceMap).thriftifiedMap;
    }

    private ISliceMap multigetSliceInternal(String keyspace, List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level, boolean thriftify)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        ThriftValidation.validatePredicate(metadata, column_parent, predicate);
        ThriftValidation.validateConsistencyLevel(keyspace, consistency_level, RequestType.READ);

        List<ReadCommand> commands = new ArrayList<ReadCommand>();
        if (predicate.column_names != null)
        {
            for (ByteBuffer key: keys)
            {
                ThriftValidation.validateKey(metadata, key);
                commands.add(new SliceByNamesReadCommand(keyspace, key, column_parent, predicate.column_names));
            }
        }
        else
        {
            SliceRange range = predicate.slice_range;
            for (ByteBuffer key: keys)
            {
                ThriftValidation.validateKey(metadata, key);
                commands.add(new SliceFromReadCommand(keyspace, key, column_parent, range.start, range.finish, range.reversed, range.count));
            }
        }

        return getSlice(commands, consistency_level, thriftify);
    }



    private ColumnOrSuperColumn internal_get(ByteBuffer key, ColumnPath column_path, ConsistencyLevel consistency_level)
    throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException
    {
        state().hasColumnFamilyAccess(column_path.column_family, Permission.READ);
        String keyspace = state().getKeyspace();

        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_path.column_family);
        ThriftValidation.validateColumnPath(metadata, column_path);
        ThriftValidation.validateConsistencyLevel(keyspace, consistency_level, RequestType.READ);

        QueryPath path = new QueryPath(column_path.column_family, column_path.column == null ? null : column_path.super_column);
        List<ByteBuffer> nameAsList = Arrays.asList(column_path.column == null ? column_path.super_column : column_path.column);
        ThriftValidation.validateKey(metadata, key);
        ReadCommand command = new SliceByNamesReadCommand(keyspace, key, path, nameAsList);

        Map<DecoratedKey, ColumnFamily> cfamilies = readColumnFamily(Arrays.asList(command), consistency_level);

        ColumnFamily cf = cfamilies.get(StorageService.getPartitioner().decorateKey(command.key));

        if (cf == null)
            throw new NotFoundException();
        List<ColumnOrSuperColumn> tcolumns = ThriftConverter.thriftifyColumnFamily(cf, command.queryPath.superColumnName != null, false);
        if (tcolumns.isEmpty())
            throw new NotFoundException();
        assert tcolumns.size() == 1;
        return tcolumns.get(0);
    }

    @Override
    public GetResult get(ByteBuffer key, ColumnPath column_path, ConsistencyLevel consistency_level, long lts)
    throws InvalidRequestException, NotFoundException, UnavailableException, TimedOutException
    {
        LamportClock.updateTime(lts);
        logger.debug("get");

        ColumnOrSuperColumn result = internal_get(key, column_path, consistency_level);
        if (logger.isTraceEnabled()) {
            logger.trace("get({}, {}, {}, {}) = {}", new Object[]{ByteBufferUtil.bytesToHex(key), column_path, consistency_level, lts, result});
        }
        return new GetResult(result, LamportClock.sendTimestamp());
    }

    @Override
    public GetCountResult get_count(ByteBuffer key, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MultigetCountResult multiget_count_by_time(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level, long chosen_time, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        assert false : "Still in progress";
        return null;

//        LamportClock.updateTime(lts);
//        logger.debug("multiget_count_by_time");
//
//        state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
//        String keyspace = state().getKeyspace();
//
//        Map<ByteBuffer, CountWithMetadata> results = new HashMap<ByteBuffer, CountWithMetadata>();
//
//        //do the multiget but dont convert to thrift so we can still access the previous values
//        ISliceMap iSliceMap = multigetSliceInternal(keyspace, keys, column_parent, predicate, consistency_level, false);
//        assert iSliceMap instanceof InternalSliceMap : "thriftified was false, so it should be an internal map";
//        Map<ByteBuffer, Collection<IColumn>> keyToColumnFamily = ((InternalSliceMap) iSliceMap).cassandraMap;
//
//        //select results for each key that were visible at the chosen_time
//        for(Entry<ByteBuffer, Collection<IColumn>> entry : keyToColumnFamily.entrySet()) {
//            ByteBuffer key = entry.getKey();
//            Collection<IColumn> columns = entry.getValue();
//
//            //excludes deleted columns from the count; calculates dependencies (including deleted columns), evt, and lvt
//            ClientContext countContext = new ClientContext(); //use a clientContext to simplify calculating deps
//            long maxEarliestValidTime = Long.MIN_VALUE;
//            long minLatestValidTime = Long.MAX_VALUE;
//            int count = 0;
//
//            List<ColumnOrSuperColumn> chosenColumns = new ArrayList<ColumnOrSuperColumn>();
//            for (IColumn column : columns) {
//                ColumnOrSuperColumn cosc = ThriftConverter.selectChosenColumn(column, chosen_time);
//                EvtAndLvt evtAndLvt = ColumnOrSuperColumnHelper.extractEvtAndLvt(cosc);
//                maxEarliestValidTime = Math.max(maxEarliestValidTime, evtAndLvt.getEarliestValidTime());
//                minLatestValidTime = Math.min(minLatestValidTime, evtAndLvt.getLatestValidTime());
//                try {
//                    countContext.addDep(key, cosc);
//                    count++;
//                } catch (NotFoundException nfe) {
//                    //TODO: Don't use exceptions for this, have a separate addDep function for serverside use
//                    //don't increment count
//                }
//            }
//
//            results.put(key, new CountWithMetadata(count, maxEarliestValidTime, minLatestValidTime, countContext.getDeps()));
//        }
//
//        if (logger.isTraceEnabled()) {
//            logger.trace("multiget_count_by_time({}, {}, {}, {}, {}) = {}", new Object[]{ByteBufferUtil.listBytesToHex(keys), column_parent, predicate, consistency_level, lts, results});
//        }
//        return new MultigetCountResult(results, LamportClock.sendTimestamp());
    }

    @Override
    public MultigetCountResult multiget_count(List<ByteBuffer> keys, ColumnParent column_parent, SlicePredicate predicate, ConsistencyLevel consistency_level, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
       throw new UnsupportedOperationException();
    }

    private void internal_insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level, Set<Dep> deps, long chosenTime)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
       throw new UnsupportedOperationException();
    }

    @Override
    public WriteResult insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level, Set<Dep> deps, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
       throw new UnsupportedOperationException();
    }

    @Override
    public long put(ByteBuffer key, String cfName, Mutation mutation, ConsistencyLevel consistency_level, List<Long> dvc) throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        VersionVector.updateGSVFromClient(dvc);
        String keyspace = state().getKeyspace();
        long dvcmax = dvc.get(0);
        for (int i = 1; i < dvc.size(); ++i) {
            if (dvc.get(i) > dvcmax)
                dvcmax = dvc.get(i);
        }
        long ut = LamportClock.updateLocalTimeIncr(dvcmax);
        long[] DV = VersionVector.GSV.clone();
        byte dc = ShortNodeId.getLocalDC();
        DV[dc] = ut;
        VersionVector.updateVV(dc, ut);
        state().hasColumnFamilyAccess(cfName, Permission.WRITE);
        // try {logger.error("Put k={} v={} t={}", new Object[]{ByteBufferUtil.string(key),ByteBufferUtil.string(mutation.column_or_supercolumn.column.value), DV});} catch(Exception ex) {}
        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, cfName);
        ThriftValidation.validateKey(metadata, key);

        RowMutation rm = new RowMutation(keyspace, key);
        ThriftValidation.validateMutation(metadata, mutation);
        rm.addColumnOrSuperColumn(cfName, mutation.column_or_supercolumn, dc, DV);

        doInsert(consistency_level, Arrays.asList(rm));
        return LamportClock.getCurrentTime();

    }

    private void internal_batch_mutate(Map<ByteBuffer,Map<String,List<Mutation>>> mutation_map, ConsistencyLevel consistency_level, long opTimestamp)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        // List<String> cfamsSeen = new ArrayList<String>();
        // List<IMutation> rowMutations = new ArrayList<IMutation>();
        // String keyspace = state().getKeyspace();
        //
        // Set<Dependency> dependencies = new HashSet<Dependency>(); //SBJ: Dummy
        // //Note, we're assuming here the entire mutation is resident on this node, (all storageProxy calls are local)
        // //the returnDeps are what we return to the client, for them to depend on after this call
        //
        // for (Map.Entry<ByteBuffer, Map<String, List<Mutation>>> mutationEntry: mutation_map.entrySet())
        // {
        //     ByteBuffer key = mutationEntry.getKey();
        //
        //     // We need to separate row mutation for standard cf and counter cf (that will be encapsulated in a
        //     // CounterMutation) because it doesn't follow the same code path
        //     RowMutation rmStandard = null;
        //     RowMutation rmCounter = null;
        //
        //     Map<String, List<Mutation>> columnFamilyToMutations = mutationEntry.getValue();
        //     for (Map.Entry<String, List<Mutation>> columnFamilyMutations : columnFamilyToMutations.entrySet())
        //     {
        //         String cfName = columnFamilyMutations.getKey();
        //
        //         // Avoid unneeded authorizations
        //         if (!(cfamsSeen.contains(cfName)))
        //         {
        //             state().hasColumnFamilyAccess(cfName, Permission.WRITE);
        //             cfamsSeen.add(cfName);
        //         }
        //
        //         CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, cfName);
        //         ThriftValidation.validateKey(metadata, key);
        //
        //         RowMutation rm;
        //         if (metadata.getDefaultValidator().isCommutative())
        //         {
        //             ThriftValidation.validateCommutativeForWrite(metadata, consistency_level);
        //             rmCounter = rmCounter == null ? new RowMutation(keyspace, key, dependencies) : rmCounter;
        //             rm = rmCounter;
        //         }
        //         else
        //         {
        //             rmStandard = rmStandard == null ? new RowMutation(keyspace, key, dependencies) : rmStandard;
        //             rm = rmStandard;
        //         }
        //
        //         for (Mutation mutation : columnFamilyMutations.getValue())
        //         {
        //             // try {
        //             //     logger.error("Batch_Mutate : key={}, mutation={} opTimestamp = {} now = {} logicalTime = {} ", new Object[]{ByteBufferUtil.string(key), mutation, opTimestamp, System.currentTimeMillis(), LamportClock.getCurrentTime()});
        //             // }catch (Exception ex) {}
        //             ThriftValidation.validateMutation(metadata, mutation);
        //
        //             if (mutation.deletion != null)
        //             {
        //                 rm.deleteColumnOrSuperColumn(cfName, mutation.deletion, opTimestamp, opTimestamp);
        //             }
        //             if (mutation.column_or_supercolumn != null)
        //             {
        //                 rm.addColumnOrSuperColumn(cfName, mutation.column_or_supercolumn, opTimestamp, opTimestamp);
        //             }
        //         }
        //     }
        //     if (rmStandard != null && !rmStandard.isEmpty())
        //         rowMutations.add(rmStandard);
        //     if (rmCounter != null && !rmCounter.isEmpty())
        //         rowMutations.add(new org.apache.cassandra.db.CounterMutation(rmCounter, consistency_level));
        // }
        //
        // doInsert(consistency_level, rowMutations);
    }

    @Override
    public BatchMutateResult batch_mutate(Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map, ConsistencyLevel consistency_level, Set<Dep> deps, long lts)
            throws InvalidRequestException, UnavailableException, TimedOutException {
       throw new UnsupportedOperationException();
    }

    private long internal_remove(ByteBuffer key, ColumnPath column_path, long timestamp, ConsistencyLevel consistency_level, Set<Dep> deps, boolean isCommutativeOp)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public WriteResult remove(ByteBuffer key, ColumnPath column_path, long timestamp, ConsistencyLevel consistency_level, Set<Dep> deps, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        LamportClock.updateTime(lts);
        logger.debug("remove");

        long remove_timestamp = internal_remove(key, column_path, timestamp, consistency_level, deps, false);
        if (timestamp == 0) {
            assert remove_timestamp > 0;
        } else {
            assert timestamp == remove_timestamp;
        }

        if (logger.isTraceEnabled()) {
            logger.trace("remove({}, {}, {}, {}, {}, {}) = {}", new Object[]{ByteBufferUtil.bytesToHex(key), column_path, timestamp, consistency_level, deps, lts, remove_timestamp});
        }
        return new WriteResult(remove_timestamp, LamportClock.sendTimestamp());
    }

    private void doInsert(ConsistencyLevel consistency_level, List<? extends IMutation> mutations) throws UnavailableException, TimedOutException, InvalidRequestException
    {
        ThriftValidation.validateConsistencyLevel(state().getKeyspace(), consistency_level, RequestType.WRITE);
        if (mutations.isEmpty())
            return;
        try
        {
            schedule(DatabaseDescriptor.getRpcTimeout());
            try
            {
                StorageProxy.mutate(mutations, consistency_level);
            }
            finally
            {
                release();
            }
        }
        catch (TimeoutException e)
        {
            logger.debug("... timed out");
            throw new TimedOutException();
        }
    }

    @Override
    public KsDef describe_keyspace(String table) throws NotFoundException, InvalidRequestException
    {
        state().hasKeyspaceSchemaAccess(Permission.READ);

        KSMetaData ksm = Schema.instance.getTableDefinition(table);
        if (ksm == null)
            throw new NotFoundException();

        return ksm.toThrift();
    }

    private List<KeySlice> getRangeSlicesInternal(ColumnParent column_parent, SlicePredicate predicate, KeyRange range, List<ByteBuffer> knownKeys, ConsistencyLevel consistency_level, Long chosen_time)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        String keyspace = state().getKeyspace();
        state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);

        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        ThriftValidation.validatePredicate(metadata, column_parent, predicate);
        ThriftValidation.validateKeyRange(metadata, column_parent.super_column, range);
        ThriftValidation.validateConsistencyLevel(keyspace, consistency_level, RequestType.READ);

        List<Row> rows;
        try
        {
            IPartitioner p = StorageService.getPartitioner();
            AbstractBounds<RowPosition> bounds;
            if (range.start_key == null)
            {
                Token.TokenFactory tokenFactory = p.getTokenFactory();
                Token left = tokenFactory.fromString(range.start_token);
                Token right = tokenFactory.fromString(range.end_token);
                bounds = Range.makeRowRange(left, right, p);
            }
            else
            {
                bounds = new Bounds<RowPosition>(RowPosition.forKey(range.start_key, p), RowPosition.forKey(range.end_key, p));
            }
            schedule(DatabaseDescriptor.getRpcTimeout());
            try
            {
                rows = StorageProxy.getRangeSlice(new RangeSliceCommand(keyspace, column_parent, predicate, bounds, range.row_filter, range.count), consistency_level);
            }
            finally
            {
                release();
            }
            assert rows != null;
        }
        catch (TimeoutException e)
        {
            logger.debug("... timed out");
            throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        //filter out knownKeys
        for (Iterator<Row> row_it = rows.iterator(); row_it.hasNext(); ) {
            Row row = row_it.next();
            if (knownKeys.contains(row.key.key)) {
                row_it.remove();
            }
        }

        List<KeySlice> result;
        if (chosen_time == null) {
            result = ThriftConverter.thriftifyKeySlices(rows, column_parent, predicate);
        } else {
            result = ThriftConverter.thriftifyKeySlicesAtTime(rows, column_parent, predicate, chosen_time);
        }
        return result;

    }

    @Override
    public GetRangeSlicesResult get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range, ConsistencyLevel consistency_level, long lts)
    throws InvalidRequestException, UnavailableException, TException, TimedOutException
    {
        LamportClock.updateTime(lts);
        logger.debug("range_slice");

        List<KeySlice> result = getRangeSlicesInternal(column_parent, predicate, range, Collections.<ByteBuffer> emptyList(), consistency_level, null);

        if (logger.isTraceEnabled()) {
            logger.trace("get_range_slices({}, {}, {}, {}, {}) = {}", new Object[]{column_parent, predicate, range, consistency_level, lts, result});
        }
        return new GetRangeSlicesResult(result, LamportClock.sendTimestamp());
    }

    @Override
    public GetRangeSlicesResult get_range_slices_by_time(ColumnParent column_parent, SlicePredicate predicate, KeyRange range, List<ByteBuffer> knownKeys, ConsistencyLevel consistency_level, long chosen_time, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        LamportClock.updateTime(lts);
        logger.debug("range_slice_by_time");

        List<KeySlice> result = getRangeSlicesInternal(column_parent, predicate, range, knownKeys, consistency_level, chosen_time);

        if (logger.isTraceEnabled()) {
            logger.trace("get_range_slices({}, {}, {}, {}, {}) = {}", new Object[]{column_parent, predicate, range, consistency_level, lts, result});
        }
        return new GetRangeSlicesResult(result, LamportClock.sendTimestamp());
    }

    @Override
    public GetIndexedSlicesResult get_indexed_slices(ColumnParent column_parent, IndexClause index_clause, SlicePredicate column_predicate, ConsistencyLevel consistency_level, long lts) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        LamportClock.updateTime(lts);
        logger.debug("scan");

        state().hasColumnFamilyAccess(column_parent.column_family, Permission.READ);
        String keyspace = state().getKeyspace();
        CFMetaData metadata = ThriftValidation.validateColumnFamily(keyspace, column_parent.column_family, false);
        ThriftValidation.validateColumnParent(metadata, column_parent);
        ThriftValidation.validatePredicate(metadata, column_parent, column_predicate);
        ThriftValidation.validateIndexClauses(metadata, index_clause);
        ThriftValidation.validateConsistencyLevel(keyspace, consistency_level, RequestType.READ);

        List<Row> rows;
        try
        {
            rows = StorageProxy.scan(keyspace,
                                     column_parent.column_family,
                                     index_clause,
                                     column_predicate,
                                     consistency_level);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (TimeoutException e)
        {
            logger.debug("... timed out");
            throw new TimedOutException();
        }
        return new GetIndexedSlicesResult(ThriftConverter.thriftifyKeySlices(rows, column_parent, column_predicate), LamportClock.sendTimestamp());
    }

    @Override
    public List<KsDef> describe_keyspaces() throws TException, InvalidRequestException
    {
        state().hasKeyspaceSchemaAccess(Permission.READ);

        Set<String> keyspaces = Schema.instance.getTables();
        List<KsDef> ksset = new ArrayList<KsDef>();
        for (String ks : keyspaces)
        {
            try
            {
                ksset.add(describe_keyspace(ks));
            }
            catch (NotFoundException nfe)
            {
                logger.info("Failed to find metadata for keyspace '" + ks + "'. Continuing... ");
            }
        }
        return ksset;
    }

    @Override
    public String describe_cluster_name() throws TException
    {
        return DatabaseDescriptor.getClusterName();
    }

    @Override
    public String describe_version() throws TException
    {
        return Constants.VERSION;
    }

    @Override
    public List<TokenRange> describe_ring(String keyspace)throws InvalidRequestException
    {
        return StorageService.instance.describeRing(keyspace);
    }

    @Override
    public String describe_partitioner() throws TException
    {
        return StorageService.getPartitioner().getClass().getName();
    }

    @Override
    public String describe_snitch() throws TException
    {
        if (DatabaseDescriptor.getEndpointSnitch() instanceof DynamicEndpointSnitch)
            return ((DynamicEndpointSnitch)DatabaseDescriptor.getEndpointSnitch()).subsnitch.getClass().getName();
        return DatabaseDescriptor.getEndpointSnitch().getClass().getName();
    }

    @Override
    public List<String> describe_splits(String cfName, String start_token, String end_token, int keys_per_split)
    throws TException, InvalidRequestException
    {
        // TODO: add keyspace authorization call post CASSANDRA-1425
        Token.TokenFactory tf = StorageService.getPartitioner().getTokenFactory();
        List<Token> tokens = StorageService.instance.getSplits(state().getKeyspace(), cfName, new Range<Token>(tf.fromString(start_token), tf.fromString(end_token)), keys_per_split);
        List<String> splits = new ArrayList<String>(tokens.size());
        for (Token token : tokens)
        {
            splits.add(tf.toString(token));
        }
        return splits;
    }

    @Override
    public long login(AuthenticationRequest auth_request, long lts) throws AuthenticationException, AuthorizationException, TException
    {
        LamportClock.updateTime(lts);
        state().login(auth_request.getCredentials());
        return LamportClock.sendTimestamp();
    }

    /**
     * Schedule the current thread for access to the required services
     */
    protected void schedule(long timeoutMS) throws TimeoutException
    {
        requestScheduler.queue(Thread.currentThread(), state().getSchedulingValue(), timeoutMS);
    }

    /**
     * Release count for the used up resources
     */
    protected void release()
    {
        requestScheduler.release();
    }

    // helper method to apply migration on the migration stage. typical migration failures will throw an
    // InvalidRequestException. atypical failures will throw a RuntimeException.
    private static void applyMigrationOnStage(final Migration m)
    {
        Future f = StageManager.getStage(Stage.MIGRATION).submit(new Callable()
        {
            @Override
            public Object call() throws Exception
            {
                m.apply();
                m.announce();
                return null;
            }
        });
        try
        {
            f.get();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized String system_add_column_family(CfDef cf_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("add_column_family");
        state().hasColumnFamilySchemaAccess(Permission.WRITE);
        CFMetaData.addDefaultIndexNames(cf_def);
        ThriftValidation.validateCfDef(cf_def, null);
        validateSchemaAgreement();

        try
        {
            cf_def.unsetId(); // explicitly ignore any id set by client (Hector likes to set zero)
            applyMigrationOnStage(new AddColumnFamily(CFMetaData.fromThrift(cf_def)));
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    @Override
    public synchronized String system_drop_column_family(String column_family)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("drop_column_family");
        state().hasColumnFamilySchemaAccess(Permission.WRITE);
        validateSchemaAgreement();

        try
        {
            applyMigrationOnStage(new DropColumnFamily(state().getKeyspace(), column_family));
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    @Override
    public synchronized String system_add_keyspace(KsDef ks_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("add_keyspace");
        state().hasKeyspaceSchemaAccess(Permission.WRITE);
        validateSchemaAgreement();
        ThriftValidation.validateKeyspaceNotYetExisting(ks_def.name);

        // generate a meaningful error if the user setup keyspace and/or column definition incorrectly
        for (CfDef cf : ks_def.cf_defs)
        {
            if (!cf.getKeyspace().equals(ks_def.getName()))
            {
                throw new InvalidRequestException("CfDef (" + cf.getName() +") had a keyspace definition that did not match KsDef");
            }
        }

        try
        {
            Collection<CFMetaData> cfDefs = new ArrayList<CFMetaData>(ks_def.cf_defs.size());
            for (CfDef cf_def : ks_def.cf_defs)
            {
                cf_def.unsetId(); // explicitly ignore any id set by client (same as system_add_column_family)
                CFMetaData.addDefaultIndexNames(cf_def);
                ThriftValidation.validateCfDef(cf_def, null);
                cfDefs.add(CFMetaData.fromThrift(cf_def));
            }

            ThriftValidation.validateKsDef(ks_def);
            applyMigrationOnStage(new AddKeyspace(KSMetaData.fromThrift(ks_def, cfDefs.toArray(new CFMetaData[cfDefs.size()]))));
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    @Override
    public synchronized String system_drop_keyspace(String keyspace)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("drop_keyspace");
        state().hasKeyspaceSchemaAccess(Permission.WRITE);
        validateSchemaAgreement();

        try
        {
            applyMigrationOnStage(new DropKeyspace(keyspace));
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    /** update an existing keyspace, but do not allow column family modifications.
     * @throws SchemaDisagreementException
     */
    @Override
    public synchronized String system_update_keyspace(KsDef ks_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("update_keyspace");
        state().hasKeyspaceSchemaAccess(Permission.WRITE);
        ThriftValidation.validateTable(ks_def.name);
        if (ks_def.getCf_defs() != null && ks_def.getCf_defs().size() > 0)
            throw new InvalidRequestException("Keyspace update must not contain any column family definitions.");
        validateSchemaAgreement();

        try
        {
            ThriftValidation.validateKsDef(ks_def);
            applyMigrationOnStage(new UpdateKeyspace(KSMetaData.fromThrift(ks_def)));
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    @Override
    public synchronized String system_update_column_family(CfDef cf_def)
    throws InvalidRequestException, SchemaDisagreementException, TException
    {
        logger.debug("update_column_family");
        state().hasColumnFamilySchemaAccess(Permission.WRITE);
        if (cf_def.keyspace == null || cf_def.name == null)
            throw new InvalidRequestException("Keyspace and CF name must be set.");
        CFMetaData oldCfm = Schema.instance.getCFMetaData(cf_def.keyspace, cf_def.name);
        if (oldCfm == null)
            throw new InvalidRequestException("Could not find column family definition to modify.");
        CFMetaData.addDefaultIndexNames(cf_def);
        ThriftValidation.validateCfDef(cf_def, oldCfm);
        validateSchemaAgreement();

        try
        {
            // ideally, apply() would happen on the stage with the
            CFMetaData.applyImplicitDefaults(cf_def);
            org.apache.cassandra.db.migration.avro.CfDef result;
            try
            {
                result = CFMetaData.fromThrift(cf_def).toAvro();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
            UpdateColumnFamily update = new UpdateColumnFamily(result);
            applyMigrationOnStage(update);
            return Schema.instance.getVersion().toString();
        }
        catch (ConfigurationException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
        catch (IOException e)
        {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        }
    }

    private void validateSchemaAgreement() throws SchemaDisagreementException
    {
        // unreachable hosts don't count towards disagreement
        Map<String, List<String>> versions = Maps.filterKeys(StorageProxy.describeSchemaVersions(),
                                                             Predicates.not(Predicates.equalTo(StorageProxy.UNREACHABLE)));
        if (versions.size() > 1)
            throw new SchemaDisagreementException();
    }

    @Override
    public long truncate(String cfname, Set<Dep> deps, long lts) throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        LamportClock.updateTime(lts);

        logger.debug("truncating {} in {}", cfname, state().getKeyspace());
        state().hasColumnFamilyAccess(cfname, Permission.WRITE);
        try
        {
            schedule(DatabaseDescriptor.getRpcTimeout());
            try
            {
                StorageProxy.truncateBlocking(state().getKeyspace(), cfname);
            }
            finally
            {
                release();
            }
        }
        catch (TimeoutException e)
        {
            logger.debug("... timed out");
            throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw (UnavailableException) new UnavailableException().initCause(e);
        }

        return LamportClock.sendTimestamp();
    }

    @Override
    public long set_keyspace(String keyspace, long lts) throws InvalidRequestException, TException
    {
        LamportClock.updateTime(lts);
        ThriftValidation.validateTable(keyspace);

        state().setKeyspace(keyspace);
        return LamportClock.sendTimestamp();
    }

    @Override
    public Map<String, List<String>> describe_schema_versions() throws TException, InvalidRequestException
    {
        logger.debug("checking schema agreement");
        return StorageProxy.describeSchemaVersions();
    }

    // counter methods

    @Override
    public WriteResult add(ByteBuffer key, ColumnParent column_parent, CounterColumn column, ConsistencyLevel consistency_level, Set<Dep> deps, long lts)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
       throw new UnsupportedOperationException();
    }

    @Override
    public WriteResult remove_counter(ByteBuffer key, ColumnPath path, ConsistencyLevel consistency_level, Set<Dep> deps, long lts)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        throw new UnsupportedOperationException();
    }

    private static String uncompress(ByteBuffer query, Compression compression) throws InvalidRequestException
    {
        String queryString = null;

        // Decompress the query string.
        try
        {
            switch (compression)
            {
                case GZIP:
                    FastByteArrayOutputStream byteArray = new FastByteArrayOutputStream();
                    byte[] outBuffer = new byte[1024], inBuffer = new byte[1024];

                    Inflater decompressor = new Inflater();

                    int lenRead = 0;
                    while (true)
                    {
                        if (decompressor.needsInput())
                            lenRead = query.remaining() < 1024 ? query.remaining() : 1024;
                            query.get(inBuffer, 0, lenRead);
                            decompressor.setInput(inBuffer, 0, lenRead);

                        int lenWrite = 0;
                        while ((lenWrite = decompressor.inflate(outBuffer)) !=0)
                            byteArray.write(outBuffer, 0, lenWrite);

                        if (decompressor.finished())
                            break;
                    }

                    decompressor.end();

                    queryString = new String(byteArray.toByteArray(), 0, byteArray.size(), "UTF-8");
                    break;
                case NONE:
                    try
                    {
                        queryString = ByteBufferUtil.string(query);
                    }
                    catch (CharacterCodingException ex)
                    {
                        throw new InvalidRequestException(ex.getMessage());
                    }
                    break;
            }
        }
        catch (DataFormatException e)
        {
            throw new InvalidRequestException("Error deflating query string.");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new InvalidRequestException("Unknown query string encoding.");
        }
        return queryString;
    }

    @Override
    public CqlResult execute_cql_query(ByteBuffer query, Compression compression)
    throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        if (logger.isDebugEnabled()) logger.debug("execute_cql_query");

        String queryString = uncompress(query,compression);

        try
        {
            return QueryProcessor.process(queryString, state());
        }
        catch (RecognitionException e)
        {
            InvalidRequestException ire = new InvalidRequestException("Invalid or malformed CQL query string");
            ire.initCause(e);
            throw ire;
        }
    }

    @Override
    public CqlPreparedResult prepare_cql_query(ByteBuffer query, Compression compression)
    throws InvalidRequestException, TException
    {
        if (logger.isDebugEnabled()) logger.debug("prepare_cql_query");

        String queryString = uncompress(query,compression);

        try
        {
            return QueryProcessor.prepare(queryString, state());
        }
        catch (RecognitionException e)
        {
            InvalidRequestException ire = new InvalidRequestException("Invalid or malformed CQL query string");
            ire.initCause(e);
            throw ire;
        }
    }

    @Override
    public CqlResult execute_prepared_cql_query(int itemId, List<String> bindVariables)
    throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException, TException
    {
        if (logger.isDebugEnabled()) logger.debug("execute_prepared_cql_query");

        CQLStatement statement = state().getPrepared().get(itemId);

        if (statement == null)
            throw new InvalidRequestException(String.format("Prepared query with ID %d not found", itemId));
        logger.trace("Retrieved prepared statement #{} with {} bind markers", itemId, state().getPrepared().size());

        return QueryProcessor.processPrepared(statement, state(), bindVariables);
    }

    private void checkPermission(Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map)
    throws InvalidRequestException
    {
        //check for permission
        List<String> cfamsSeen = new ArrayList<String>();

        for (Map<String, List<Mutation>> columnFamilyToMutations: mutation_map.values()) {
            for (String cfName : columnFamilyToMutations.keySet()) {
                // Avoid unneeded authorizations
                if (!(cfamsSeen.contains(cfName))) {
                    state().hasColumnFamilyAccess(cfName, Permission.WRITE);
                    cfamsSeen.add(cfName);
                }
            }
        }

        //WL TODO: Also do thrift validation here too so it throws errors at the correct place
    }

    @Override
    public short transactional_batch_mutate_cohort(Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map, ByteBuffer coordinator_key, long transaction_id, long lts)
    throws InvalidRequestException, TException
    {
        LamportClock.updateTime(lts);
        logger.debug("transactional_batch_mutate_cohort");

        String keyspace;
        try {
            keyspace = state().getKeyspace();
            checkPermission(mutation_map);
        } catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        }

        BatchMutateTransactionCohort cohort = new BatchMutateTransactionCohort();
        cohort.receiveLocalTransaction(keyspace, mutation_map, coordinator_key, transaction_id);

        return 0;
    }

    @Override
    public BatchMutateResult transactional_batch_mutate_coordinator(Map<ByteBuffer, Map<String, List<Mutation>>> mutation_map, ConsistencyLevel consistency_level, Set<Dep> deps, ByteBuffer coordinator_key, Set<ByteBuffer> all_keys, long transaction_id, long lts)
    throws InvalidRequestException, UnavailableException, TimedOutException, TException
    {
        LamportClock.updateTime(lts);
        logger.debug("transactional_batch_mutate_coordinator");

        ByteBuffer originalCoordinatorKey = ByteBufferUtil.hexToBytes(ByteBufferUtil.bytesToHex(coordinator_key));

        assert consistency_level == ConsistencyLevel.LOCAL_QUORUM || consistency_level == ConsistencyLevel.ONE;

        String keyspace = state().getKeyspace();
        checkPermission(mutation_map);
        Set<Dependency> dependencies = new HashSet<Dependency>();
        for (Dep dep : deps) {
            dependencies.add(new Dependency(dep));
        }

        BatchMutateTransactionCoordinator coordinator = new BatchMutateTransactionCoordinator();
        try {
            coordinator.receiveLocalTransaction(keyspace, mutation_map, dependencies, coordinator_key, all_keys, transaction_id);
        } catch (IOException e) {
            InvalidRequestException ex = new InvalidRequestException(e.getMessage());
            ex.initCause(e);
            throw ex;
        } catch (TimeoutException e) {
            logger.debug("... timed out");
            throw new TimedOutException();
        }
        long timestamp = coordinator.waitForLocalCommitNoInterruption();

        //the entire BMT is a single op, so it only create a single dependency
        Set<Dep> new_deps = Collections.singleton(new Dep(originalCoordinatorKey, timestamp));

        if (logger.isTraceEnabled()) {
            logger.trace("transactional_batch_mutate_coordinator({}, {}, {}, {}) = {}", new Object[]{mutation_map, consistency_level, deps, lts, new_deps});
        }
        return new BatchMutateResult(new_deps, LamportClock.sendTimestamp());
    }

    // main method moved to CassandraDaemon
}
