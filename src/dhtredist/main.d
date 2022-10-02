/*******************************************************************************

    Tool to initiate a redistribution of data within a dht. The standard use
    case, when adding new nodes to a dht, is as follows:
        1. Set up the new nodes as required. You can initially set their hash
           ranges, in config.ini, to null (that is, min=0xffffffffffffffff,
           max=0x0000000000000000), indicating that they are empty. (This is
           optional; dhtredist does not handle empty nodes in any special way.)
        2. Generate a nodes xml file containing the address/port of all nodes,
           including the new ones to be added to the dht.
        3. Run dhtredist, passing it the created xml file.

    copyright:
        Copyright (c) 2014-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtredist.main;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dhtredist.Redist;


/*******************************************************************************

    Main function.

    Params:
        args = array with raw command line arguments

*******************************************************************************/

version (unittest) {} else
private int main ( string[] args )
{
    auto app = new DhtRedist;
    return app.main(args);
}
