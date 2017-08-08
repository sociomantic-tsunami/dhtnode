/*******************************************************************************

    Table of request handlers by command.

    copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.node.RequestHandlers;

import swarm.neo.node.ConnectionHandler;
import dhtproto.common.RequestCodes;

import GetHashRange = dhtnode.request.neo.GetHashRange;
import Put = dhtnode.request.neo.Put;
import Get = dhtnode.request.neo.Get;
import Mirror = dhtnode.request.neo.Mirror;
import GetAll = dhtnode.request.neo.GetAll;

/*******************************************************************************

    This table of request handlers by command is used by the connection handler.
    When creating a new request, the function corresponding to the request
    command is called in a fiber.

*******************************************************************************/

public ConnectionHandler.CmdHandlers request_handlers;

static this ( )
{
    request_handlers[RequestCode.GetHashRange] = &GetHashRange.handle;
    request_handlers[RequestCode.Put] = &Put.handle;
    request_handlers[RequestCode.Get] = &Get.handle;
    request_handlers[RequestCode.Mirror] = &Mirror.handle;
    request_handlers[RequestCode.GetAll] = &GetAll.handle;

    request_handlers.rehash;
}
