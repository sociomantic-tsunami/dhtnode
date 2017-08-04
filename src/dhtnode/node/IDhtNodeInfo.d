/*******************************************************************************

    DHT node information interface

    copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.node.IDhtNodeInfo;


/*******************************************************************************

    Imports

*******************************************************************************/

import swarm.node.model.IChannelsNodeInfo;


/*******************************************************************************

    IDhtNodeInfo, extends IChannelsNodeInfo with getters for the DHT node's hash
    range.

*******************************************************************************/

public interface IDhtNodeInfo : IChannelsNodeInfo
{
    /***************************************************************************

        Returns:
            Minimum hash supported by DHT node.

    ***************************************************************************/

    public hash_t min_hash ( );


    /***************************************************************************

        Returns:
            Maximum hash supported by DHT node.

    ***************************************************************************/

    public hash_t max_hash ( );


    /***************************************************************************

        DHT node state enum

    ***************************************************************************/

    public enum State
    {
        Running,
        Terminating,
        ShutDown
    }


    /***************************************************************************

        Returns:
            state of node

    ***************************************************************************/

    public State state ( );
}

