/*******************************************************************************

    DHT node shared resource manager. Handles acquiring / relinquishing of
    global resources by active request handlers.

    copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.connection.SharedResources;



/*******************************************************************************

    Imports

    Imports which are required by the ConnectionResources struct, below, are
    imported publicly, as they are also needed in
    dhtnode.request.model.RequestResources (which imports this
    module). This is done to simplify the process of modifying the fields of
    ConnectionResources --  forgetting to import something into both modules
    is a common source of very confusing compile errors.

*******************************************************************************/

import ocean.transition;

import swarm.common.connection.ISharedResources;

public import ocean.io.select.client.FiberSelectEvent;
public import ocean.io.select.client.FiberTimerEvent;

public import swarm.common.request.helper.LoopCeder;

public import dhtnode.storage.StorageEngineStepIterator;

public import dhtnode.connection.DhtClient;

public import swarm.util.RecordBatcher;

public import dhtproto.client.legacy.common.NodeRecordBatcher : NodeRecordBatcherMap;

public import dhtproto.node.request.params.RedistributeNode;



/*******************************************************************************

    Struct whose fields define the set of shared resources which can be acquired
    by a request. Each request can acquire a single instance of each field.

*******************************************************************************/

public struct ConnectionResources
{
    mstring channel_buffer;
    mstring key_buffer;
    mstring filter_buffer;
    mstring batch_buffer;
    mstring value_buffer;
    cstring[] channel_list_buffer;
    hash_t[] hash_buffer;
    FiberSelectEvent event;
    FiberTimerEvent timer;
    LoopCeder loop_ceder;
    StorageEngineStepIterator iterator;
    RecordBatcher batcher;
    RecordBatch record_batch;
    NodeRecordBatcherMap node_record_batch;
    RedistributeNode[] redistribute_node_buffer;
    DhtClient dht_client;
}



/*******************************************************************************

    Mix in a class called SharedResources which contains a free list for each of
    the fields of ConnectionResources. The free lists are used by
    individual requests to acquire and relinquish resources required for
    handling.

*******************************************************************************/

mixin SharedResources_T!(ConnectionResources);

