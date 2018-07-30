/*******************************************************************************

    Implementation of DHT 'GetVersion' request

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.request.GetVersionRequest;

/*******************************************************************************

    Imports

*******************************************************************************/

import Protocol = dhtproto.node.request.GetVersion;

/*******************************************************************************

    Request handler. Does nothing because version handling is completely
    implemented in protocol.

*******************************************************************************/

public scope class GetVersionRequest : Protocol.GetVersion
{
    import dhtnode.request.model.ConstructorMixin;

    /***************************************************************************

        Adds this.resources and constructor to initialize it and forward
        arguments to base

    ***************************************************************************/

    mixin RequestConstruction!();
}
