/*******************************************************************************

    Implementation of DHT 'GetChannelsRequest' request

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.GetChannelsRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dhtproto.node.request.GetChannels;

import ocean.transition;


/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class GetChannelsRequest : Protocol.GetChannels
{
    import dhtnode.request.model.ConstructorMixin;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Must return list of all channels stored in this node.

        Returns:
            value_getter_dg = The delegate that is called with the list of
                              channel names.

    ***************************************************************************/

    final override protected void getChannelsIds (
        scope void delegate ( const(void)[] ) value_getter_dg )
    {
        foreach (channel; this.resources.storage_channels)
            value_getter_dg(channel.id);
    }
}
