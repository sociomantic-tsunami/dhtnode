/*******************************************************************************

    Table of request handlers by command.

    copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.node.RequestHandlers;

import swarm.neo.node.ConnectionHandler;
import swarm.neo.request.Command;
import dhtproto.common.RequestCodes;

import dhtnode.request.neo.GetHashRange;
import dhtnode.request.neo.Put;
import dhtnode.request.neo.Exists;
import dhtnode.request.neo.Get;
import dhtnode.request.neo.Mirror;
import dhtnode.request.neo.GetAll;
import dhtnode.request.neo.GetChannels;
import dhtnode.request.neo.Remove;
import dhtnode.request.neo.RemoveChannel;
import dhtnode.request.neo.Update;

/*******************************************************************************

    This table of request handlers by command is used by the connection handler.
    When creating a new request, the function corresponding to the request
    command is called in a fiber.

*******************************************************************************/

public ConnectionHandler.RequestMap requests;

static this ( )
{
    requests.add(Command(RequestCode.GetHashRange, 0), "GetHashRange",
        GetHashRangeImpl_v0.classinfo, false);
    requests.add(Command(RequestCode.Put, 0), "Put",
        PutImpl_v0.classinfo, true);
    requests.add(Command(RequestCode.Exists, 0), "Exists",
        ExistsImpl_v0.classinfo, true);
    requests.add(Command(RequestCode.Get, 0), "Get",
        GetImpl_v0.classinfo, true);
    requests.add(Command(RequestCode.Update, 0), "Update",
        UpdateImpl_v0.classinfo, true);
    requests.add(Command(RequestCode.Mirror, 0), "Mirror",
        MirrorImpl_v0.classinfo, false);
    requests.add(Command(RequestCode.GetAll, 0), "GetAll",
        GetAllImpl_v0.classinfo, false);
    requests.add(Command(RequestCode.GetChannels, 0), "GetChannels",
        GetChannelsImpl_v0.classinfo, false);
    requests.add(Command(RequestCode.Remove, 0), "Remove",
        RemoveImpl_v0.classinfo, true);
    requests.add(Command(RequestCode.RemoveChannel, 0), "RemoveChannel",
        RemoveChannelImpl_v0.classinfo, false);
}
