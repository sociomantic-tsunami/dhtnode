/*******************************************************************************

    Implementation of DHT 'GetChannelSize' request

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.GetChannelSizeRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dhtproto.node.request.GetChannelSize;

import ocean.transition;

/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class GetChannelSizeRequest : Protocol.GetChannelSize
{
    import dhtnode.request.model.ConstructorMixin;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Gets the size metadata for specified channel.

        Params:
            channel_name = name of channel to be queried

        Returns:
            size data aggregated in a struct

    ***************************************************************************/

    final override protected ChannelSizeData getChannelData (
        cstring channel_name)
    {
        ulong records, bytes;

        auto  storage_channel =
            *this.resources.channel_buffer in this.resources.storage_channels;
        if (storage_channel !is null)
        {
            records = storage_channel.num_records;
            bytes   = storage_channel.num_bytes;
        }

        return ChannelSizeData(
            this.resources.node_info.node_item.Address,
            this.resources.node_info.node_item.Port,
            records,
            bytes
            );
    }
}
