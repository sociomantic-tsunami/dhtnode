/*******************************************************************************

    Server config class for use with ocean.util.config.ClassFiller.

    copyright:
        Copyright (c) 2012-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.config.ServerConfig;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import ConfigReader = ocean.util.config.ConfigFiller;

/*******************************************************************************

    Server config values

*******************************************************************************/

public class ServerConfig
{
    ConfigReader.Required!(mstring) address;

    ConfigReader.Required!(ushort) port;

    ConfigReader.Required!(mstring) minval;

    ConfigReader.Required!(mstring) maxval;

    cstring data_dir = "data";

    uint connection_limit = 5_000;

    uint backlog = 2048;
}
