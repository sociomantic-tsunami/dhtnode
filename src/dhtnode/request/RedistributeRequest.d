/*******************************************************************************

    Implementation of DHT 'Redistribute' request

    A Redistribute request instructs the node to change its hash responsibility
    range and to forward any records for which it is no longer responsible to
    another node. The client sending this request is required to include a list
    of replacement nodes, along with their hash responsibility ranges.

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.RedistributeRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import Protocol = dhtproto.node.request.Redistribute;

import dhtnode.storage.StorageEngine;

import ocean.util.log.Logger;

/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dhtnode.request.RedistributeRequest");
}

/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class RedistributeRequest : Protocol.Redistribute
{
    import dhtproto.node.request.params.RedistributeNode;
    import dhtproto.client.legacy.DhtConst;

    import dhtnode.connection.DhtClient;
    import dhtnode.node.RedistributionProcess : redistribution_process;
    import dhtnode.storage.StorageEngineStepIterator;
    import dhtnode.request.model.ConstructorMixin;

    import Hash = swarm.util.Hash;
    import swarm.Const : NodeItem;
    import dhtproto.client.legacy.DhtConst;
    import dhtproto.client.legacy.common.NodeRecordBatcher;
    import dhtproto.client.legacy.internal.registry.model.IDhtNodeRegistryInfo;
    import dhtproto.client.legacy.internal.connection.model.IDhtNodeConnectionPoolInfo;

    import ocean.core.Array : copy;
    import ocean.core.Verify;
    import ocean.core.TypeConvert : downcast;
    import ocean.time.StopWatch;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Adjust storage resources if necessary to handle given hash range for
        upcoming redistribution

        Params:
            min = minimal hash value for expected dataset
            max = maximal hash value for expected dataset

    ***************************************************************************/

    final override protected void adjustHashRange ( hash_t min, hash_t max )
    {
        log.info("Setting hash range: 0x{:X16}..0x{:X16}", min, max);
        this.resources.storage_channels.hash_range.set(min, max);
    }

    /***************************************************************************

        Process actual redistribution in an implementation-defined way

    ***************************************************************************/

    final override protected void redistributeData(RedistributeNode[] dataset)
    {
        redistribution_process.starting();
        scope ( exit )
        {
            redistribution_process.finishing();
        }

        // Inform the storage channels about the nodes being forwarded to
        foreach ( node; *this.resources.redistribute_node_buffer )
        {
            this.resources.storage_channels.hash_range.newNodeAdded(
                node.node, node.range.min, node.range.max);
        }

        // We don't want events on the connection handler socket (to the dht
        // client) to mess with the fiber any more, as it's about to start being
        // managed by the ScopeRequests instance of the internal dht client (see
        // the call to DhtClient.perform() in sendBatch(), below). Simply
        // unregistering the socket from epoll prevents any unexpected events
        // from interrupting the flow of the request.
        this.reader.fiber.unregister();

        // set up dht client
        auto client = this.resources.dht_client;
        foreach ( node; *this.resources.redistribute_node_buffer )
        {
            client.addNode(node.node, node.range);
        }

        this.resources.node_record_batch.reset(
            cast(IDhtNodeRegistryInfo)client.nodes);

        // iterate over channels, redistributing data
        foreach ( channel; this.resources.storage_channels )
        {
            auto dht_channel = downcast!(StorageEngine)(channel);
            verify(dht_channel !is null);
            try
            {
                this.handleChannel(client, dht_channel);
            }
            catch ( Exception e )
            {
                log.error("Exception thrown while redistributing channel '{}': "
                    "'{}' @ {}:{}", channel.id, e.message, e.file, e.line);
                throw e;
            }
        }
    }

    /***************************************************************************

        Code indicating the result of forwarding a record. See forwardRecord().

    ***************************************************************************/

    private enum ForwardResult
    {
        None,
        Invalid,
        Batched,
        SentBatch,
        SentSingle,
        SendError
    }

    /***************************************************************************

        Iterates over the given storage engine, forwarding those records for
        which this node is no longer responsible to the appropriate node. If an
        error occurs while forwarding one or more records, those records are
        kept in the storage engine and the complete iteration is retried.

        Params:
            client = dht client instance to send data to other nodes
            channel = storage channel to process

    ***************************************************************************/

    private void handleChannel ( DhtClient client, StorageEngine channel )
    {
        log.info("Redistributing channel '{}'", channel.id);

        bool error_during_iteration;
        do
        {
            error_during_iteration = false;
            ulong num_records_before = channel.num_records;
            ulong num_records_iterated, num_records_sent;

            this.resources.iterator.setStorage(channel);
            this.resources.iterator.next(); // advance iterator to first record

            while ( !this.resources.iterator.lastKey )
            {
                bool remove_record;
                NodeItem node;

                if ( this.recordShouldBeForwarded(
                    this.resources.iterator.key_as_string, client, node) )
                {
                    auto result = this.forwardRecord(client, channel,
                        this.resources.iterator.key_as_string,
                        this.resources.iterator.value, node);
                    with ( ForwardResult ) final switch ( result )
                    {
                        case SentSingle:
                        case Invalid:
                            remove_record = true;
                            break;
                        case Batched:
                            num_records_sent++;
                            break;
                        case SentBatch:
                            break;
                        case SendError:
                            error_during_iteration = true;
                            break;
                        case None:
                            verify(false);
                            break;
                        version (D_Version2){} else
                        {
                            default: assert(false);
                        }
                    }
                }

                this.advanceIteration(this.resources.iterator, remove_record,
                    channel);

                if ( num_records_iterated % 100_000 == 0 )
                {
                    log.trace("Progress redistributing channel '{}': {}/{} "
                        "records iterated, {} forwarded, channel now contains "
                        "{} records",
                        channel.id, num_records_iterated + 1, num_records_before,
                        num_records_sent, channel.num_records);
                }

                num_records_iterated++;

                this.resources.loop_ceder.handleCeding();
            }

            if ( !this.flushBatches(client, channel) )
            {
                error_during_iteration = true;
            }

            if ( error_during_iteration )
            {
                const uint retry_s = 2;

                log.error("Finished redistributing channel '{}': {}/{} records "
                    "iterated, channel now contains {} records, "
                    " (error occurred during iteration over channel, retrying in {}s)",
                    channel.id, num_records_iterated, num_records_before,
                    channel.num_records, retry_s);

                this.resources.timer.wait(retry_s);
            }
            else
            {
                log.info("Finished redistributing channel '{}': {}/{} records "
                    "iterated, channel now contains {} records",
                    channel.id, num_records_iterated, num_records_before,
                    channel.num_records);
            }
        }
        while ( error_during_iteration );
    }


    /***************************************************************************

        Determines whether the specified record should be forwarded to another
        node of whether this node is still responsible for it. If it should be
        forwarded, the output value node is set to the address/port of the node
        which should receive it.

        Params:
            key = record key
            client = dht client instance (for node registry)
            node = out value which receives the address/port of the node which
                is responsible for this record, if it should be forwarded

        Returns:
            true if this record should be forwarded (in which case the address
            and port of the node to which it should be sent are stored in the
            out value node), or false if it should be kept by this node

    ***************************************************************************/

    private bool recordShouldBeForwarded ( cstring key, DhtClient client,
        out NodeItem node )
    {
        auto hash = Hash.straightToHash(key);
        foreach ( n; client.nodes )
        {
            auto dht_node = cast(IDhtNodeConnectionPoolInfo)n;
            if ( Hash.isWithinNodeResponsibility(
                hash, dht_node.min_hash, dht_node.max_hash) )
            {
                node.Address = n.address;
                node.Port = n.port;
                return true;
            }
        }

        return false;
    }


    /***************************************************************************

        Relocates a record by adding it to a batch to be compressed and sent to
        the specified node. If the current batch of records to that node is
        full, then sendBatch() is called, sending the whole batch to the node.

        Three obscure cases have special handling:
            1. In the case of a record of 0 length, the Invalid result is
               returned, which will cause the record to be removed without being
               forwarded.
            2. In the case of a record of length larger than the fixed size
               limit, the Invalid result is returned, which will cause the
               record to be removed without being forwarded.
            3. In the case of a record which is of a valid size but is too big
               to fit inside the batch buffer (even if it's empty), the
               individual record is sent uncompressed, using a standard Put
               request.

        Params:
            client = dht client instance to send data to other nodes
            channel = name of storage channel to which the record belongs
            key = record key
            value = record value
            node = address/port of node to which record should be forwarded

        Returns:
            enum value indicating whether the record was added to a batch, sent
            individually, sent as part of a batch, or sent and encountered an
            I/O error

    ***************************************************************************/

    private ForwardResult forwardRecord ( DhtClient client, StorageEngine channel,
        cstring key, cstring value, NodeItem node )
    {
        if ( value.length == 0 )
        {
            log.warn("Removing empty record {}", key);
            return ForwardResult.Invalid;
        }

        if ( value.length >= DhtConst.RecordSizeLimit )
        {
            log.warn("Removing too large record ({} bytes) {}", value.length,
                key);
            return ForwardResult.Invalid;
        }

        auto batch = this.resources.node_record_batch[node];
        bool fits, too_big;
        fits = batch.fits(key, value, too_big);

        if ( too_big )
        {
            log.warn("Forwarding large record {} ({} bytes) individually", key,
                value.length);
            return this.sendRecord(client, key, value, channel)
                ? ForwardResult.SentSingle : ForwardResult.SendError;
        }
        else
        {
            ForwardResult result = ForwardResult.Batched;

            if ( !fits )
            {
                result = this.sendBatch(client, batch, channel)
                    ? ForwardResult.SentBatch : ForwardResult.SendError;

                // The batch is always cleared. If an error occurred, we just
                // retry the whole iteration.
                batch.clear();

                verify(batch.fits(key, value));
            }

            auto add_result = batch.add(key, value);
            verify(add_result == add_result.Added);

            return result;
        }
    }


    /***************************************************************************

        Called at the end of an iteration over a channel. Flushes any partially
        built-up batches of records to the appropriate nodes.

        Params:
            client = dht client instance to send data to other nodes
            channel = storage channel to which the records belong

        Returns:
            true if flushing succeeded, false if an error occurred during the
            forwarding of a batch

    ***************************************************************************/

    private bool flushBatches ( DhtClient client, StorageEngine channel )
    {
        bool send_error;

        foreach ( node; client.nodes )
        {
            auto node_item = NodeItem(node.address, node.port);
            auto batch = this.resources.node_record_batch[node_item];

            if ( batch.length )
            {
                if ( !this.sendBatch(client, batch, channel) )
                {
                    send_error = true;
                }
                batch.clear();
            }
        }

        return !send_error;
    }


    /***************************************************************************

        Compresses and forwards the specified batch of records to the node which
        is now responsible for it.

        If the batch is sent successfully, the records it contained are removed
        from the storage engine. Upon error, do not attempt to retry sending the
        records immediately -- the return value indicates that the complete
        iteration over this channel's data should be repeated (see
        handleChannel()).

        Params:
            client = dht client instance to send data to other nodes
            batch = batch of records to compress and send
            channel = storage channel to which the records belong

        Returns:
            true if the batch was successfully forwarded or false if an error
            occurred

    ***************************************************************************/

    private bool sendBatch ( DhtClient client, NodeRecordBatcher batch,
        StorageEngine channel )
    {
        bool error;

        NodeRecordBatcher put_dg ( client.RequestContext )
        {
            return batch;
        }

        void notifier ( client.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                log.error("Error while sending batch of {} records to channel '{}': {}",
                    batch.length, channel.id, info.message(client.msg_buf));
                error = true;
            }
        }

        client.perform(this.reader.fiber, client.putBatch(batch.address,
            batch.port, channel.id, &put_dg, &notifier));

        // Remove successfully sent records from channel
        if ( !error )
        {
            foreach ( hash; batch.batched_hashes )
            {
                channel.remove(hash);
            }
        }

        return !error;
    }


    /***************************************************************************

        Forwards the specified record to the node which is now responsible for
        it.

        If the record is sent successfully, it will be removed from the storage
        engine after advancing the iterator (see advanceIteration()). Upon
        error, do not attempt to retry sending the record immediately -- the
        return value indicates that the complete iteration over this channel's
        data should be repeated (see handleChannel()).

        Params:
            client = dht client instance to send data to other nodes
            key = record key
            value = record value
            channel = storage channel to which the record belongs

        Returns:
            true if the record was successfully forwarded or false if an error
            occurred

    ***************************************************************************/

    private bool sendRecord ( DhtClient client, cstring key, cstring value,
        StorageEngine channel )
    {
        bool error;

        cstring put_dg ( client.RequestContext )
        {
            return value;
        }

        void notifier ( client.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                log.error("Error while sending record {} to channel '{}': {}",
                    key, channel.id, info.message(client.msg_buf));
                error = true;
            }
        }

        auto hash = Hash.straightToHash(key);
        client.perform(this.reader.fiber, client.put(channel.id, hash, &put_dg,
            &notifier));

        return !error;
    }


    /***************************************************************************

        Advances the provided iterator to the next record, removing the current
        record from the storage engine if required.

        Note that the removal of a record is performed *after* the iterator has
        been advanced. This is necessary in order to keep the iteration
        consistent.

        Params:
            iterator = iterator over current storage engine
            remove_record = indicates that the record pointed at by the
                iterator should be removed after iteration
            channel = storage engine being iterated

    ***************************************************************************/

    private void advanceIteration ( StorageEngineStepIterator iterator,
        bool remove_record, StorageEngine channel )
    {
        hash_t key;
        if ( remove_record )
        {
            key = iterator.key();
        }

        iterator.next();

        if ( remove_record )
        {
            channel.remove(key);
        }
    }
}
