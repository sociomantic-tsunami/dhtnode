/*******************************************************************************

    Implementation of DHT 'GetSize' request

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.GetSizeRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dhtproto.node.request.GetSize;

/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class GetSizeRequest : Protocol.GetSize
{
    import dhtnode.request.model.ConstructorMixin;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Must return aggregated size of all channels.

        Returns:
            metadata that includes the size

    ***************************************************************************/

    final override protected SizeData getSizeData ( )
    {
        ulong records, bytes;

        foreach ( channel; this.resources.storage_channels )
        {
            auto channel_records = channel.num_records;
            auto channel_bytes = channel.num_bytes;

            records += channel_records;
            bytes += channel_bytes;
        }

        return SizeData(
            this.resources.node_info.node_item.Address,
            this.resources.node_info.node_item.Port,
            records,
            bytes
        );
    }
}
