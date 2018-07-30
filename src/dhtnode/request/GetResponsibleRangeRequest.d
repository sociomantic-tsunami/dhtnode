/*******************************************************************************

    Implementation of DHT 'GetResponsibleRange' request

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.GetResponsibleRangeRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dhtproto.node.request.GetResponsibleRange;

/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class GetResponsibleRangeRequest : Protocol.GetResponsibleRange
{
    import dhtnode.request.model.ConstructorMixin;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************

        Must return minimum and maximum allowed hash value this node
        is responsible for.

        Params:
            min = minimal allowed hash
            max = maximal allowed hash

    ***************************************************************************/

    final override protected void getRangeLimits ( out hash_t min, out hash_t max )
    {
        min = this.resources.node_info.min_hash;
        max = this.resources.node_info.max_hash;
    }
}
