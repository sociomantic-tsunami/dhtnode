/*******************************************************************************

    DHT node test runner

    Imports the DHT test from dhtproto and runs it on the real DHT node.

    copyright:
        Copyright (c) 2015-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhttest.main;

/*******************************************************************************

    Imports

*******************************************************************************/

import dhttest.TestRunner;
import turtle.runner.Runner;

import ocean.transition;

/*******************************************************************************

    Test runner which spawns a real DHT node to run tests on.

*******************************************************************************/

private class RealDhtTestRunner : DhtTestRunner
{
    /***************************************************************************

        Copies the DHT node's config file to the sandbox before starting the
        node.

    ***************************************************************************/

    override public CopyFileEntry[] copyFiles ( )
    {
        return [
            CopyFileEntry("test/dhttest/etc/config.ini", "etc/config.ini"),
            CopyFileEntry("test/dhttest/etc/credentials", "etc/credentials")
        ];
    }


    /***************************************************************************

        Override the super class' method to specify the dhtnode's required
        arguments.

    ***************************************************************************/

    override protected void configureTestedApplication ( out double delay,
        out istring[] args, out istring[istring] env )
    {
        super.configureTestedApplication(delay, args, env);

        args = ["--config=etc/config.ini"];
    }
}

/*******************************************************************************

    Main function. Forwards arguments to test runner.

*******************************************************************************/

int main ( istring[] args )
{
    auto runner = new TurtleRunner!(RealDhtTestRunner)("dhtnode", "dhttest.cases");
    return runner.main(args);
}
