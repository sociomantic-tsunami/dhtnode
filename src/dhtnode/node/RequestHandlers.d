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

public ConnectionHandler.CmdHandlers requests;

static this ( )
{
    requests.add(RequestCode.GetHashRange, "GetHashRange", &GetHashRange.handle);
    requests.add(RequestCode.Put, "Put", &Put.handle);
    requests.add(RequestCode.Get, "Get", &Get.handle);
    requests.add(RequestCode.Mirror, "Mirror", &Mirror.handle);
    requests.add(RequestCode.GetAll, "GetAll", &GetAll.handle);
}
