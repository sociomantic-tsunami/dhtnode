/*******************************************************************************

    Implementation of DHT 'GetAllKeys' request

    copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.GetAllKeysRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dhtproto.node.request.GetAllKeys;

/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class GetAllKeysRequest : Protocol.GetAllKeys
{
    import dhtnode.request.model.IterationMixin;
    import dhtnode.request.model.ConstructorMixin;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Adds this.iterator and prepareChannel override to initialize it.
        Defines default `getNext` method

    ***************************************************************************/

    mixin ChannelIteration!(resources, IterationKind.Key);
}
