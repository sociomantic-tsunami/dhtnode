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
    requests.addHandler!(GetHashRangeImpl_v0)();
    requests.addHandler!(PutImpl_v0)();
    requests.addHandler!(ExistsImpl_v0)();
    requests.addHandler!(GetImpl_v0)();
    requests.addHandler!(UpdateImpl_v0)();
    requests.addHandler!(MirrorImpl_v0)();
    requests.addHandler!(GetAllImpl_v0)();
    requests.addHandler!(GetChannelsImpl_v0)();
    requests.addHandler!(RemoveImpl_v0)();
    requests.addHandler!(RemoveChannelImpl_v0)();
}
