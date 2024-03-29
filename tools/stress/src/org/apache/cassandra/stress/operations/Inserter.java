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
package org.apache.cassandra.stress.operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.client.ClientLibrary;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.stress.Session;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.LamportClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class Inserter extends Operation
{
    private static Logger logger = LoggerFactory.getLogger(Inserter.class);
    private static List<ByteBuffer> values;

    public Inserter(Session client, int index)
    {
        super(client, index);
    }

    @Override
    public void run(ClientLibrary clientLibrary) throws IOException
    {
        if (values == null)
            values = generateValues();

        List<Column> columns = new ArrayList<Column>();
        List<SuperColumn> superColumns = new ArrayList<SuperColumn>();

        // format used for keys
        String format = "%0" + session.getTotalKeysLength() + "d";

        // for (int i = 0; i < session.getColumnsPerKey(); i++)
        // {
        //     columns.add(new Column(columnName(i, session.timeUUIDComparator))
        //                         .setValue(values.get(i % values.size()))
        //                         .setTimestamp(FBUtilities.timestampMicros()));
        // }
        long time = LamportClock.now()/100;
        ByteBuffer val = ByteBufferUtil.bytes(String.format("%0"+ session.getColumnSize()+"d", time));
        Column column = new Column(columnName(0, session.timeUUIDComparator)).setValue(val).setTimestamp(FBUtilities.timestampMicros());
        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
        {
            // supers = [SuperColumn('S' + str(j), columns) for j in xrange(supers_per_key)]
            for (int i = 0; i < session.getSuperColumns(); i++)
            {
                String superColumnName = "S" + Integer.toString(i);
                superColumns.add(new SuperColumn(ByteBufferUtil.bytes(superColumnName), columns));
            }
        }

        String rawKey = String.format(format, index);
        ByteBuffer key = ByteBufferUtil.bytes(rawKey);
        Mutation mutation = new Mutation().setColumn_or_supercolumn(new ColumnOrSuperColumn().setColumn(column));

        long start = System.currentTimeMillis();

        boolean success = false;
        String exceptionMessage = null;

        for (int t = 0; t < session.getRetryTimes(); t++)
        {
            if (success)
                break;

            try
            {
                // We're just populating the cluster, so we dont care about deps yet
		        clientLibrary.getContext().clearDeps();
                clientLibrary.put(key, "Standard1", mutation);
                success = true;
            }
            catch (Exception e)
            {
                exceptionMessage = getExceptionMessage(e);
                success = false;
            }
        }

        if (!success)
        {
            logger.error(String.format("Operation [%d] retried %d times - error inserting key %s %s%n",
                                index,
                                session.getRetryTimes(),
                                rawKey,
                                (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        session.operations.getAndIncrement();
        session.keys.getAndIncrement();
        session.latency.getAndAdd(System.currentTimeMillis() - start);
    }


    @Override
    public void run(Cassandra.Client client) throws IOException
    {
        if (values == null)
            values = generateValues();

        List<Column> columns = new ArrayList<Column>();
        List<SuperColumn> superColumns = new ArrayList<SuperColumn>();

        // format used for keys
        String format = "%0" + session.getTotalKeysLength() + "d";

        for (int i = 0; i < session.getColumnsPerKey(); i++)
        {
            columns.add(new Column(columnName(i, session.timeUUIDComparator))
                                .setValue(values.get(i % values.size()))
                                .setTimestamp(FBUtilities.timestampMicros()));
        }

        if (session.getColumnFamilyType() == ColumnFamilyType.Super)
        {
            // supers = [SuperColumn('S' + str(j), columns) for j in xrange(supers_per_key)]
            for (int i = 0; i < session.getSuperColumns(); i++)
            {
                String superColumnName = "S" + Integer.toString(i);
                superColumns.add(new SuperColumn(ByteBufferUtil.bytes(superColumnName), columns));
            }
        }

        String rawKey = String.format(format, index);
        Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

        record.put(ByteBufferUtil.bytes(rawKey), session.getColumnFamilyType() == ColumnFamilyType.Super
                                                                                ? getSuperColumnsMutationMap(superColumns)
                                                                                : getColumnsMutationMap(columns));

        long startNano = System.nanoTime();

        boolean success = false;
        String exceptionMessage = null;

        for (int t = 0; t < session.getRetryTimes(); t++)
        {
            if (success)
                break;

            try
            {
                client.batch_mutate(record, session.getConsistencyLevel(), session.getPregeneratedDependencies(), LamportClock.COPS_UNSUPPORTED);
                success = true;
            }
            catch (Exception e)
            {
                exceptionMessage = getExceptionMessage(e);
                success = false;
            }
        }

        if (!success)
        {
            logger.error(String.format("Operation [%d] retried %d times - error inserting key %s %s%n",
                                index,
                                session.getRetryTimes(),
                                rawKey,
                                (exceptionMessage == null) ? "" : "(" + exceptionMessage + ")"));
        }

        session.operations.getAndIncrement();
        session.keys.getAndIncrement();
        long latencyNano = System.nanoTime() - startNano;
        session.latency.getAndAdd(latencyNano/1000000);
        session.latencies.add(latencyNano/1000);
    }

    private Map<String, List<Mutation>> getSuperColumnsMutationMap(List<SuperColumn> superColumns)
    {
        List<Mutation> mutations = new ArrayList<Mutation>();
        Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();

        for (SuperColumn s : superColumns)
        {
            ColumnOrSuperColumn superColumn = new ColumnOrSuperColumn().setSuper_column(s);
            mutations.add(new Mutation().setColumn_or_supercolumn(superColumn));
        }

        mutationMap.put("Super1", mutations);

        return mutationMap;
    }

    private Map<String, List<Mutation>> getColumnsMutationMap(List<Column> columns)
    {
        List<Mutation> mutations = new ArrayList<Mutation>();
        Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();

        for (Column c : columns)
        {
            ColumnOrSuperColumn column = new ColumnOrSuperColumn().setColumn(c);
            mutations.add(new Mutation().setColumn_or_supercolumn(column));
        }

        mutationMap.put("Standard1", mutations);

        return mutationMap;
    }
}
