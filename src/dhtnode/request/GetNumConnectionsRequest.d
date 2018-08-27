/*******************************************************************************

    Implementation of DHT 'GetNumConnections' request

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.GetNumConnectionsRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dhtproto.node.request.GetNumConnections;

/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class GetNumConnectionsRequest : Protocol.GetNumConnections
{
    import dhtnode.request.model.ConstructorMixin;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();
 
    /***************************************************************************

        Must return total num_conns of established connections to this node.

        Returns:
            metadata that includes number of established connections

    ***************************************************************************/

    final override protected NumConnectionsData getConnectionsData ( )
    {
        return NumConnectionsData(
            this.resources.node_info.node_item.Address,
            this.resources.node_info.node_item.Port,
            this.resources.node_info.num_open_connections
        );
    }
}
