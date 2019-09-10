/*******************************************************************************

    Implementation of DHT 'GetAllFilter' request

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.GetAllFilterRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import Protocol = dhtproto.node.request.GetAllFilter;

/*******************************************************************************

    Request handler

*******************************************************************************/

public scope class GetAllFilterRequest : Protocol.GetAllFilter
{
    import dhtnode.request.model.IterationMixin;
    import dhtnode.request.model.ConstructorMixin;

    import ocean.text.Search;

    /***************************************************************************

        Sub-string search instance.

    ***************************************************************************/

    private SearchFruct match;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();

    /***************************************************************************
    
        Predicate that accepts records that match filter defined by this.match

    ***************************************************************************/

    private bool filterPredicate ( cstring key, cstring value )
    {
        return this.match.forward(value) < value.length;
    }

    /***************************************************************************

        Adds this.iterator and prepareChannel override to initialize it
        Defines `getNext` that uses filterPredicate to filter records

    ***************************************************************************/

    mixin ChannelIteration!(resources, IterationKind.KeyValue, filterPredicate);

    /***************************************************************************
        
        Initialized regex match based on provided filter string

        Params:
            filter = filter string

    ***************************************************************************/

    final override protected void prepareFilter ( cstring filter )
    {
        this.match = search(filter);
    }
}
