/*******************************************************************************

    Class to manage the range of hashes handled by a DHT node, including
    the ability to modify the range and update the config file with the new
    range.

    The hash range takes one of two forms:
        1. The standard form. Min hash <= max hash.
        2. Empty. Min hash and max hash both have magic values (see
           ocean.math.Range), allowing this state to be distinguished.

    The empty state is supported to allow new nodes to be started up with no
    current hash responsibility, awaiting an external command to tell them which
    range they should support. It could also be used to effectively delete a
    node by setting its hash range to empty.

    copyright:
        Copyright (c) 2014-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.node.DhtHashRange;



/*******************************************************************************

    Imports

*******************************************************************************/

import dhtnode.config.HashRangeConfig;

import swarm.util.Hash : HashRange;

import ocean.core.Enforce;
import ocean.core.Verify;



public class DhtHashRange
{
    /***************************************************************************

        Min & max hash values.

    ***************************************************************************/

    private HashRange range_;


    /***************************************************************************

        Config file updater.

    ***************************************************************************/

    private HashRangeConfig config_file;


    /***************************************************************************

        Constructor. Sets the range as specified.

        Params:
            min = min hash
            max = max hash
            config_file = config file updater

        Throws:
            if the range specified by min & max is invalid

    ***************************************************************************/

    public this(hash_t min, hash_t max, HashRangeConfig config_file)
    {
        verify(config_file !is null);
        this.config_file = config_file;

        enforce(HashRange.isValid(min, max), "Invalid hash range");
        this.range_ = HashRange(min, max);
    }


    /***************************************************************************

        Returns:
            hash range

    ***************************************************************************/

    public HashRange range ( )
    {
        return this.range_;
    }


    /***************************************************************************

        Returns:
            true if the hash range is empty

    ***************************************************************************/

    public bool is_empty ( )
    {
        return this.range.is_empty;
    }


    /***************************************************************************

        Sets the hash range and updates the config file(s).

        Params:
            min = min hash
            max = max hash

        Throws:
            if the specified range is invalid

    ***************************************************************************/

    public void set (hash_t min, hash_t max)
    {
        this.config_file.set(min, max);

        enforce(HashRange.isValid(min, max), "Invalid hash range");
        this.range_ = HashRange(min, max);
    }


    /***************************************************************************

        Sets the hash range to empty and updates the config file(s).

    ***************************************************************************/

    public void clear ( )
    {
        this.config_file.clear();
        this.range_ = this.range_.init;
    }
}

