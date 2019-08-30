/*******************************************************************************

    Implementation of DHT 'Listen' request

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.ListenRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dhtproto.node.request.Listen;

import dhtnode.storage.StorageEngine;

import ocean.util.log.Logger;
import ocean.transition;

/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dhtnode.request.ListenRequest");
}

/*******************************************************************************

    Listen request

*******************************************************************************/

public scope class ListenRequest : Protocol.Listen, StorageEngine.IListener
{
    import dhtnode.request.model.ConstructorMixin;

    import ocean.core.Verify;
    import ocean.core.Enforce;
    import ocean.core.TypeConvert : downcast;
    import ocean.core.Array : pop;

    /***************************************************************************

        Storage channel being read from. The reference is only set once the
        request begins processing.

    ***************************************************************************/

    private StorageEngine storage_channel;

    /***************************************************************************

        Set to true when the waitEvents method is waiting for the fiber select
        event to be triggered.

    ***************************************************************************/

    private bool waiting_for_trigger;

    /***************************************************************************

        Flags used to communicate event outcomes from event callback to other
        class methods.

    ***************************************************************************/

    private bool finish_trigger, flush_trigger;

    /***************************************************************************

        Maximum length of the internal list of record hashes which have been
        modified and thus need to be forwarded to the listening client. A
        maximum is given to avoid the (presumably extreme) situation where the
        hash buffer is growing indefinitely.

    ***************************************************************************/

    private static immutable HashBufferMaxLength = (1024 / hash_t.sizeof) * 256; // 256 Kb of hashes

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Ensures that requested channel exists and can be read from. Memorizes
        storage channel.

        Params:
            channel_name = name of channel to be prepared

        Return:
            `true` if it is possible to proceed with Listen request

    ***************************************************************************/

    final override protected bool prepareChannel ( cstring channel_name )
    {
        this.storage_channel = downcast!(StorageEngine)(
            this.resources.storage_channels.getCreate(channel_name));

        if (this.storage_channel is null)
            return false;

        // unregistered in this.finalizeRequest
        this.storage_channel.registerListener(this);

        return true;
    }

    /***************************************************************************

        Must provide next new DHT record or indicate if it is impossible

        Params:
            channel_name = name of channel to check for new records
            key = slice from HexDigest buffer. Must be filled with record key
                data if it exists. Must not be resized.
            value = must be filled with record value if it exists

        Return:
            'true' if it was possible to get the record, 'false' if more waiting
            is necessary or channel got deleted

    ***************************************************************************/

    final override protected bool getNextRecord ( cstring channel_name,
        mstring key, out Const!(void)[] value )
    {
        enforce(key.length == Hash.HashDigits);

        hash_t hash;

        if ((*this.resources.hash_buffer).pop(hash))
        {
            Hash.toHexString(hash, key);

            // Get record from storage engine
            mstring value_slice;
            this.storage_channel.get(key, *this.resources.value_buffer,
                value_slice);
            value = value_slice;

            this.resources.node_info.record_action_counters
                .increment("forwarded", value.length);
            this.resources.loop_ceder.handleCeding();

            return true;
        }

        return false;
    }

    /***************************************************************************

        This method gets called to wait for new DHT records and/or report
        any other pending events

        Params:
            finish = indicates if request needs to be ended
            flush =  indicates if socket needs to be flushed

    ***************************************************************************/

    final override protected void waitEvents ( out bool finish, out bool flush )
    {
        scope(exit)
        {
            finish = this.finish_trigger;
            flush  = this.flush_trigger;
            this.finish_trigger = false;
            this.flush_trigger = false;
        }

        // have already recevied some event by that point
        if (this.finish_trigger || this.flush_trigger)
            return;

        this.waiting_for_trigger = true;
        scope(exit) this.waiting_for_trigger = false;

        this.resources.event.wait;
    }

    /***************************************************************************

        IListener interface method. Called when a record in the listened channel
        has changed, the write buffer needs flushing, or the listener should
        finish.

        Params:
            code = trigger type
            key = key of put record
            value = put record value

    ***************************************************************************/

    public void trigger ( Code code, hash_t key )
    {
        final switch (code) with (Code)
        {
            case DataReady:
                if ( (*this.resources.hash_buffer).length < HashBufferMaxLength )
                {
                    //This could lead to that the buffer containing the same key
                    //several times. Since the buffer is flushed often and
                    //checking the value before adding it could be a to heavy
                    //cost there's no need to do a check before adding the key.
                    (*this.resources.hash_buffer) ~= key;
                }
                else
                {
                    cstring addr;
                    ushort port;
                    if ( this.reader.addr_port !is null )
                    {
                        addr = this.reader.addr_port.address;
                        port = this.reader.addr_port.port;
                    }
                    log.warn(
                        "Listen request on channel '{}', client {}:{}," ~
                            " hash buffer reached maximum length --" ~
                            " record discarded",
                        *this.resources.channel_buffer, addr, port
                    );
                }
                break;

            case Flush:
                this.flush_trigger = true;
                break;

            case Finish:
                this.finish_trigger = true;
                break;

            case Deletion:
                // Not relevant to this request.
                break;

            case None:
                verify(false);
                break;

            version (D_Version2){} else
            {
                default: assert(false);
            }
        }

        if (this.waiting_for_trigger)
        {
            this.resources.event.trigger;
        }
    }

    /***************************************************************************

        Action to trigger when a disconnection is detected.

    ***************************************************************************/

    final override protected void onDisconnect ( )
    {
        if (this.waiting_for_trigger)
        {
            this.resources.event.trigger;
        }
    }

    /***************************************************************************

        Called upon termination of the request, any cleanup steps can be put
        here.

    ***************************************************************************/

    final override protected void finalizeRequest ( )
    {
        this.storage_channel.unregisterListener(this);
    }
}
