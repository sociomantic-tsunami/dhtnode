/*******************************************************************************

    Simple client for testing neo requests.

    copyright:
        Copyright (c) 2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module neotest.main;

import ocean.transition;
import ocean.io.Stdout;
import ocean.text.convert.Integer;
import ocean.task.Task;
import ocean.task.Scheduler;
import ocean.task.util.Timer;
import swarm.neo.client.requests.NotificationFormatter;

import dhtproto.client.DhtClient;

import swarm.neo.client.requests.NotificationFormatter;

class DhtTest : Task
{
    import swarm.neo.authentication.HmacDef: Key;
    import swarm.neo.AddrPort;

    protected DhtClient dht;

    this ( )
    {
        auto auth_name = "neotest";
        ubyte[] auth_key = Key.init.content;
        this.dht = new DhtClient(theScheduler.epoll, auth_name, auth_key,
            &this.connNotifier);
        this.dht.neo.enableSocketNoDelay();
        this.dht.neo.addNode("127.0.0.1", 10_100);
    }

    override public void run ( )
    {
        try
        {
            this.dht.blocking.waitAllHashRangesKnown();
            Stdout.formatln("All nodes connected");
            this.go();
        }
        finally
        {
            Stderr.formatln("Shutting down epoll");
            theScheduler.shutdown();
        }
    }

    private void connNotifier ( DhtClient.Neo.DhtConnNotification info )
    {
        with ( info.Active ) switch ( info.active )
        {
            case connected:
                Stdout.formatln("Connected to {}:{}",
                    info.connected.node_addr.address_bytes, info.connected.node_addr.port);
                break;

            case hash_range_queried:
                Stdout.formatln("Got hash-range of {}:{}",
                    info.hash_range_queried.node_addr.address_bytes, info.hash_range_queried.node_addr.port);
                break;

            case connection_error:
                Stderr.formatln("Connection error '{}' on {}:{}",
                    getMsg(info.connection_error.e),
                    info.connection_error.node_addr.address_bytes, info.connection_error.node_addr.port);
                break;

            default:
                assert(false);
        }
    }

    abstract protected void go ( );
}

class Put : DhtTest
{
    override protected void go ( )
    {
        auto res = this.dht.blocking.put("test".dup, 0, "hello".dup);
        assert(res.succeeded);
        Stdout.formatln("Put succeeded");
    }
}

class Fill : DhtTest
{
    private hash_t max;

    private cstring[] channels;

    this ( hash_t max, cstring[] channels )
    {
        this.max = max;

        if ( channels.length == 0 )
            this.channels = ["test".dup];
        else
            this.channels = channels;
    }

    override protected void go ( )
    {
        for ( hash_t k = 0; k < this.max; k++ )
        {
            foreach ( channel; this.channels )
            {
                auto res = this.dht.blocking.put(channel, k, "hello".dup);
                assert(res.succeeded);
                if ( (k + 1) % 100 == 0 )
                    Stdout.formatln("{} Puts succeeded", k + 1);
            }
        }
    }
}

class Fetch : DhtTest
{
    private hash_t max;

    this ( hash_t max )
    {
        this.max = max;
    }

    override protected void go ( )
    {
        void[] buf;
        for ( hash_t k = 0; k < this.max; k++ )
        {
            auto res = this.dht.blocking.get("test".dup, k, buf);
            assert(res.succeeded);
            wait(500_000);
            if ( (k + 1) % 100 == 0 )
                Stdout.formatln("{} Gets succeeded", k + 1);
        }
    }
}

class Get : DhtTest
{
    override protected void go ( )
    {
        void[] buf;
        auto res = this.dht.blocking.get("test".dup, 0, buf);
        assert(res.succeeded);
        Stdout.formatln("Get succeeded, value: {}", cast(mstring)res.value);
    }
}

class GetAll : DhtTest
{
    uint c;

    override protected void go ( )
    {
        this.dht.neo.getAll("test", &this.notifier);
        this.suspend();
        Stdout.formatln("GetAll finished after receiving {} records", this.c);
        this.c = 0;
    }

    private void notifier ( DhtClient.Neo.GetAll.Notification info,
        DhtClient.Neo.GetAll.Args args )
    {
        with ( info.Active ) switch ( info.active )
        {
            case received:
                this.c++;
                if ( this.c % 100  == 0 )
                    Stdout.formatln("Received {} records", this.c);
                break;

            case finished:
                this.resume();
                break;

            default:
                break;
        }
    }
}

class Mirror : DhtTest
{
    uint c;

    override protected void go ( )
    {
        DhtClient.Neo.Mirror.Settings s;
        s.periodic_refresh_s = 5;
        this.dht.neo.mirror("test", &this.notifier, s);
        this.suspend();
        Stdout.formatln("Mirror finished after receiving {} records", this.c);
        this.c = 0;
    }

    private void notifier ( DhtClient.Neo.Mirror.Notification info,
        DhtClient.Neo.Mirror.Args args )
    {
        mstring buf;
        //~ Stdout.formatln(formatNotification(info, buf));
        with ( info.Active ) switch ( info.active )
        {
            case updated:
                this.c++;
                if ( this.c % 100 == 0 )
                    Stdout.formatln("{} Mirror updates", this.c);
                break;

            case refreshed:
                this.c++;
                if ( this.c % 1000 == 0 )
                    Stdout.formatln("{} Mirror updates", this.c);
                break;

            case channel_removed:
                this.resume();
                break;

            case updates_lost:
                Stdout.formatln("Updates lost! :(");
                break;

            default:
                break;
        }
    }
}

class MirrorFill : DhtTest
{
    override protected void go ( )
    {
        this.dht.neo.mirror("test", &this.notifier);

        for ( hash_t k = 0; k < hash_t.max; k++ )
        {
            auto res = this.dht.blocking.put("test", k, "hello".dup);
            assert(res.succeeded);
            if ( (k + 1) % 100 == 0 )
                Stdout.formatln("{} Puts succeeded", k + 1);
        }
    }

    private void notifier ( DhtClient.Neo.Mirror.Notification info,
        DhtClient.Neo.Mirror.Args args )
    {
        mstring buf;
        Stdout.formatln(formatNotification(info, buf));
    }
}

class MultiMirror : DhtTest
{
    private cstring[] channels;

    public this ( cstring[] channels )
    {
        this.channels = channels;
    }

    override protected void go ( )
    {
        foreach ( channel; this.channels )
            this.dht.neo.mirror(channel, &this.notifier);
        this.suspend();
        Stdout.formatln("At least one Mirror ended");
    }

    private void notifier ( DhtClient.Neo.Mirror.Notification info,
        DhtClient.Neo.Mirror.Args args )
    {
        mstring buf;
        Stdout.formatln("{}: {}", args.channel, formatNotification(info, buf));
        with ( info.Active ) switch ( info.active )
        {
            case channel_removed:
                this.resume();
                break;

            default:
                break;
        }
    }
}

void main ( cstring[] args )
{
    assert(args.length >= 2);

    SchedulerConfiguration config;
    initScheduler(config);

    auto cmd = args[1];
    switch ( cmd )
    {
        case "put":
            assert(args.length == 2);
            theScheduler.schedule(new Put);
            break;

        case "get":
            assert(args.length == 2);
            theScheduler.schedule(new Get);
            break;

        case "getall":
            assert(args.length == 2);
            theScheduler.schedule(new GetAll);
            break;

        case "mirror":
            assert(args.length == 2);
            theScheduler.schedule(new Mirror);
            break;

        case "mirrorfill":
            assert(args.length == 2);
            theScheduler.schedule(new MirrorFill);
            break;

        case "multimirror":
            assert(args.length >= 3);
            theScheduler.schedule(new MultiMirror(args[2..$]));
            break;

        case "fill":
            assert(args.length >= 3);
            hash_t max;
            toInteger(args[2], max);
            theScheduler.schedule(new Fill(max, args[3..$]));
            break;

        case "fetch":
            assert(args.length == 3);
            hash_t max;
            toInteger(args[2], max);
            theScheduler.schedule(new Fetch(max));
            break;

        default:
            Stderr.formatln("Unknown command '{}'", cmd);
    }

    theScheduler.eventLoop();
}
