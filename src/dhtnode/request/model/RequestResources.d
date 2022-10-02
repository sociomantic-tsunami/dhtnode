/*******************************************************************************

    Interface and base class containing getter methods to acquire
    resources needed by a DHT node request. Multiple calls to the same
    getter only result in the acquiring of a single resource of that type, so
    that the same resource is used over the life time of a request. When a
    request resource instance goes out of scope all required resources are
    automatically relinquished.

    copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.model.RequestResources;



/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.common.request.model.IRequestResources;

import dhtnode.connection.SharedResources;

import dhtnode.storage.StorageChannels;

import dhtnode.node.IDhtNodeInfo;

import dhtproto.node.request.model.DhtCommand;



/*******************************************************************************

    Mix in an interface called IRequestResources which contains a getter method
    for each type of acquirable resource, as defined by the SharedResources
    class (dhtnode.connection.SharedResources).

*******************************************************************************/

mixin IRequestResources_T!(SharedResources);



/*******************************************************************************

    Interface which extends the base IRequestResources, adding a couple of
    DHT-specific getters. It also includes DhtCommand.Resources which
    is necessary for protocol classes.

*******************************************************************************/

public interface IDhtRequestResources : IRequestResources, DhtCommand.Resources
{
    /***************************************************************************

        Local type re-definitions.

    ***************************************************************************/

    alias .FiberSelectEvent FiberSelectEvent;
    alias .LoopCeder LoopCeder;
    alias .StorageChannels StorageChannels;
    alias .IDhtNodeInfo IDhtNodeInfo;


    /***************************************************************************

        Storage channels getter.

    ***************************************************************************/

    StorageChannels storage_channels ( );


    /***************************************************************************

        Node info getter.

    ***************************************************************************/

    IDhtNodeInfo node_info ( );
}



/*******************************************************************************

    Mix in a class called RequestResources which implements
    IRequestResources. Note that this class does not implement the additional
    methods required by IDhtRequestResources -- this is done by the derived
    class in dhtnode.connection.DhtConnectionHandler.

*******************************************************************************/

mixin RequestResources_T!(SharedResources);

