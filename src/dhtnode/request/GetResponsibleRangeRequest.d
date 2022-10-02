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

public class GetResponsibleRangeRequest : Protocol.GetResponsibleRange
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
            value_getter_dg = The delegate that is called with the minimum and
                              the maximum allowed hashes.

    ***************************************************************************/

    final override protected void getRangeLimits (
        scope void delegate ( hash_t min, hash_t max ) value_getter_dg )
    {
        value_getter_dg(this.resources.node_info.min_hash,
            this.resources.node_info.max_hash);
    }
}
