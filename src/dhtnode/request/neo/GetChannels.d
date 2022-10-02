/*******************************************************************************

    GetChannels request implementation.

    copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.neo.GetChannels;

import dhtproto.node.neo.request.GetChannels;

import dhtnode.connection.neo.SharedResources;
import dhtnode.storage.StorageEngine;

import swarm.neo.node.RequestOnConn;
import swarm.neo.request.Command;

import ocean.transition;
import ocean.core.TypeConvert : castFrom, downcast;
import ocean.core.Verify;

/*******************************************************************************

    DHT node implementation of the v0 GetChannels request protocol.

*******************************************************************************/

public class GetChannelsImpl_v0 : GetChannelsProtocol_v0
{
    import dhtproto.common.RequestCodes;

    /// Request code / version. Required by ConnectionHandler.
    static immutable Command command = Command(RequestCode.GetChannels, 0);

    /// Request name for stats tracking. Required by ConnectionHandler.
    static immutable string name = "GetChannels";

    /// Flag indicating whether timing stats should be gathered for requests of
    /// this type.
    static immutable bool timing = false;

    /// Flag indicating whether this request type is scheduled for removal. (If
    /// true, clients will be warned.)
    static immutable bool scheduled_for_removal = false;

    /***************************************************************************

        opApply iteration over the names of the channels in storage.

    ***************************************************************************/

    protected override int opApply ( scope int delegate ( ref cstring ) dg )
    {
        auto resources_ =
            downcast!(SharedResources.RequestResources)(this.resources);
        verify(resources_ !is null);

        foreach ( channel; resources_.storage_channels )
        {
            cstring const_channel = channel.id;
            if ( auto ret = dg(const_channel) )
                break;
        }
        return 0;
    }
}
