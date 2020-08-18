/*******************************************************************************

    Dht node channel dump tool.

    copyright:
        Copyright (c) 2014-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.dhtdump.main;

import Version;

import dhtnode.dhtdump.DumpCycle;
import dhtnode.dhtdump.DumpStats;

import dhtproto.client.DhtClient;
import dhtproto.client.legacy.internal.helper.RetryHandshake;
import dhtproto.client.legacy.internal.registry.DhtNodeRegistry;

import ocean.io.select.EpollSelectDispatcher;
import ocean.io.select.client.TimerEvent;
import ocean.math.random.Random;
import ocean.time.StopWatch;
import ocean.transition;
import ocean.util.app.DaemonApp;
import ConfigReader = ocean.util.config.ConfigFiller;
import ocean.util.log.Logger;

import core.thread;
import core.time;

private Logger log;

static this ( )
{
    log = Log.lookup("dhtnode.dhtdump.main");
}


/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts dhtdump.

    Params:
        cl_args = array with raw command line arguments

*******************************************************************************/

version (UnitTest) {} else
private int main ( istring[] cl_args )
{
    try
    {
        auto app = new DhtDump;
        auto ret = app.main(cl_args);
        log.info("Exiting with return code {}", ret);
        return ret;
    }
    catch ( Throwable e )
    {
        log.error("Caught exception in main: {} @ {}:{}",
            e.message, e.file, e.line);
        throw e;
    }
}


public class DhtDump : DaemonApp
{
    /***************************************************************************

        Epoll selector instance

    ***************************************************************************/

    private EpollSelectDispatcher epoll;


    /***************************************************************************

        Dht client instance

    ***************************************************************************/

    private DumpCycle.ScopeDhtClient dht;


    /***************************************************************************

        Dump cycle instance.

    ***************************************************************************/

    private DumpCycle dump_cycle;


    /***************************************************************************

        Dump stats instance.

    ***************************************************************************/

    private DumpStats dump_stats;


    /***************************************************************************

        Dht settings, read from config file

    ***************************************************************************/

    private static class DhtConfig
    {
        mstring address;
        ushort port;
    }

    private DhtConfig dht_config;


    /***************************************************************************

        Dump settings, read from config file

    ***************************************************************************/

    private DumpCycle.Config dump_config;


    /***************************************************************************

        Stats log settings, read from config file

    ***************************************************************************/

    private DumpStats.Config stats_config;


    /***************************************************************************

        Constructor.

    ***************************************************************************/

    public this ( )
    {
        this.epoll = new EpollSelectDispatcher;

        static immutable app_name = "dhtdump";
        static immutable app_desc = "iterates over all channels in a dht node, dumping the"
            ~ " data to disk";
        super(app_name, app_desc, version_info);

        this.dht = new DumpCycle.ScopeDhtClient(this.epoll,
            new DhtClient.ScopeRequestsPlugin);

        this.dump_cycle = new DumpCycle(this.epoll, this.dht);
    }


    /***************************************************************************

        Set up the arguments parser for the app. The "config" argument has
        already been set up by the super class (DaemonApp), but we need to
        modify the settings so that "config" is a required argument (i.e. with
        no default value).

        Params:
            app = application instance
            argument = arguments parser to initialise

    ***************************************************************************/

    public override void setupArgs ( IApplication app, Arguments args )
    {
        args("oneshot").aliased('o').
            help("one-shot mode, perform a single dump immediately then exit");
        args("config").deefalts = null;
        args("config").required;
    }


    /***************************************************************************

        Do the actual application work. Called by the super class.

        Params:
            args = command line arguments
            config = parser instance with the parsed configuration

        Returns:
            status code to return to the OS

    ***************************************************************************/

    protected override int run ( Arguments args, ConfigParser config )
    {
        ConfigReader.fill("Dht", this.dht_config, config);
        ConfigReader.fill("Dump", this.dump_config, config);
        ConfigReader.fill("Stats", this.stats_config, config);

        this.dump_stats = new DumpStats(this.stats_ext.stats_log);

        this.initDht();

        if ( args.exists("oneshot") )
            this.dump_cycle.one_shot = true;

        this.startEventHandling(this.epoll);
        this.dump_cycle.start(this.dump_config, this.dump_stats);
        this.epoll.eventLoop();

        return true;
    }


    /***************************************************************************

        Called by the timer extension when the stats period fires. Writes the
        stats, if in cyclic mode.

    ***************************************************************************/

    override protected void onStatsTimer ( )
    {
        if ( !this.dump_cycle.one_shot )
            this.dump_stats.log();
    }


    /***************************************************************************

        Sets up the dht client for use, adding the config-specified node to the
        registry and performing the handshake. This method only exits once the
        handshake has been completed successfully.

    ***************************************************************************/

    private void initDht ( )
    {
        this.dht.addNode(this.dht_config.address, this.dht_config.port);

        static immutable retry_wait_s = 2;
        bool error;

        void result ( DhtClient.RequestContext, bool success )
        {
            if ( !success )
            {
                auto dht_registry = cast(DhtNodeRegistry)this.dht.nodes;
                if ( !(dht_registry.all_node_ranges_known &&
                       dht_registry.all_versions_ok &&
                       !dht_registry.node_range_overlap &&
                       dht_registry.node_range_gap) )
                {
                    error = true;
                }
            }
        }

        void notifier ( DhtClient.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                log.error("Error during dht handshake: {}, retrying in {}s",
                    info.message(this.dht.msg_buf), retry_wait_s);
            }
        }

        do
        {
            error = false;
            this.dht.nodeHandshake(&result, &notifier);
            this.epoll.eventLoop();

            if ( error )
            {
                // no fibers in existence yet, so we can just do a blocking wait
                Thread.sleep(seconds(retry_wait_s));
            }
        }
        while ( error );
    }
}
