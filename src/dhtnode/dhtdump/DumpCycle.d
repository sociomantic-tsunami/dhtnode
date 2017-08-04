/*******************************************************************************

    Class encapsulating the sequence of events encompassing the ongoing dump
    cycle:
        1. Intial random wait
        2. Get channel names
        3. Dump each channel to disk, in turn
        4. Wait for next cycle to begin.
        5. Return to 2.

    copyright:
        Copyright (c) 2014-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.dhtdump.DumpCycle;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dhtnode.dhtdump.DumpStats;

import dhtnode.storage.DumpFile;

import ocean.core.Array : appendCopy, copy;
import ocean.core.Enforce;

import ocean.io.select.EpollSelectDispatcher;

import ocean.io.select.client.FiberTimerEvent;

import ocean.io.select.fiber.SelectFiber;

import ocean.text.util.DigitGrouping : BitGrouping;

import dhtproto.client.DhtClient;

import ocean.io.FilePath;

import ocean.core.Array : contains;

import ocean.math.random.Random;

import ocean.time.StopWatch;
import ocean.time.WallClock;

import ocean.util.log.Log;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;

static this ( )
{
    log = Log.lookup("dhtnode.dhtdump.DumpCycle");
}



public class DumpCycle : SelectFiber
{
    import core.memory;

    /***************************************************************************

        Dump cycle settings

    ***************************************************************************/

    public static class Config
    {
        cstring data_dir = "data";
        uint period_s = 60 * 60 * 4;
        uint min_wait_s = 60;
        uint retry_wait_s = 30;
        bool disable_direct_io = false;
    }

    private Config dump_config;


    /***************************************************************************

        Flag indicating that a single dump cycle should be performed immediately
        (without initial pause) and exit afterwards.

    ***************************************************************************/

    public bool one_shot;


    /***************************************************************************

        Dht client instance

    ***************************************************************************/

    public alias ExtensibleDhtClient!(DhtClient.ScopeRequestsPlugin) ScopeDhtClient;

    private ScopeDhtClient dht;


    /***************************************************************************

        Path to write dump files to

    ***************************************************************************/

    private FilePath root;


    /***************************************************************************

        Path of current dump file

    ***************************************************************************/

    private FilePath path;


    /***************************************************************************

        Path of file being swapped (see swapNewAndBackupDumps())

    ***************************************************************************/

    private FilePath swap_path;


    /***************************************************************************

        List of channels being iterated over

    ***************************************************************************/

    private mstring[] channels;


    /***************************************************************************

        List of channels iterated over by the last cycle. Used to detect when a
        channel is removed.

    ***************************************************************************/

    private mstring[] last_cycle_channels;


    /***************************************************************************

        Dump file

    ***************************************************************************/

    private ChannelDumper file;


    /***************************************************************************

        Fiber-suspending timer event. Used to wait between dump cycles.

    ***************************************************************************/

    private FiberTimerEvent timer;


    /***************************************************************************

        Dump stats instance, passed to start()

    ***************************************************************************/

    private DumpStats stats;


    /***************************************************************************

        Buffer used for message formatting in wait()

    ***************************************************************************/

    private mstring bytes_buf;


    /***************************************************************************

        Constructor.

        Params:
            epoll = epoll instance
            dht = scope-requests dht client instance

    ***************************************************************************/

    public this ( EpollSelectDispatcher epoll, ScopeDhtClient dht )
    {
        const fiber_stack_bytes = 32 * 1024;
        super(epoll, &this.run, fiber_stack_bytes);

        this.dht = dht;

        this.root = new FilePath;
        this.path = new FilePath;
        this.swap_path = new FilePath;

        this.timer = new FiberTimerEvent(this);
    }


    /***************************************************************************

        Starts the dump cycle.

        Params:
            dump_config = dump cycle configuration parameters

    ***************************************************************************/

    public void start ( Config dump_config, DumpStats stats )
    {
        this.dump_config = dump_config;
        this.stats = stats;

        auto buffer = cast(ubyte[]) GC.malloc(IOBufferSize)[0 .. IOBufferSize];
        this.file = new ChannelDumper(buffer, NewFileSuffix,
                dump_config.disable_direct_io);

        this.root.set(this.dump_config.data_dir);
        enforce(this.root.exists, "Data directory does not exist");

        super.start();
    }


    /***************************************************************************

        Fiber method. In the normal mode, cycles infinitely, periodically
        dumping dht channels to disk. In one-shot mode, performs a single cycle
        then exits.

    ***************************************************************************/

    private void run ( )
    {
        if ( this.one_shot )
        {
            bool ok;
            do
            {
                ulong microsecs;

                ok = this.cycle(microsecs);
                if ( !ok )
                {
                    this.wait(microsecs, true);
                }
            }
            while ( !ok );
        }
        else
        {
            this.initialWait();

            while ( true )
            {
                bool ok;
                ulong microsecs;

                ok = this.cycle(microsecs);
                this.wait(microsecs, !ok);
            }
        }
    }


    /***************************************************************************

        Performs a single dump cycle, getting the list of channels from the dht
        node then writing them all to disk.

        Params:
            microsecs = (output) number of microseconds taken by the cycle

        Returns:
            true if the cycle succeeded, false if an error occurred

    ***************************************************************************/

    private bool cycle ( out ulong microsecs )
    {
        try
        {
            StopWatch time;
            time.start;

            bool error;
            auto channels = this.getChannels(error);

            log.info("Dumping {} channels", channels.length);

            foreach ( channel; channels )
            {
                log.info("Dumping '{}'", channel);

                try
                {
                    this.dumpChannel(channel, error);
                }
                catch ( Exception e )
                {
                    log.error("Exception thrown while dumping channel '{}': '{}' @ {}:{}",
                        channel, getMsg(e), e.file, e.line);
                    throw e;
                }
            }

            microsecs = time.microsec;
            this.stats.dumpedAll(microsecs / 1_000);
            return !error;
        }
        catch ( Exception e )
        {
            log.error("Exception thrown in dump cycle: '{}' @ {}:{}",
                getMsg(e), e.file, e.line);
            throw e;
        }
    }


    /***************************************************************************

        Updates the list of channels to be dumped and checks for channels which
        have been removed.

        Params:
            error = set to true if an error occurs while getting the list of
                channels

        Returns:
            list of dht channels

    ***************************************************************************/

    private Const!(mstring[]) getChannels ( ref bool error )
    {
        // Copy the last cycle's list of channels
        this.last_cycle_channels.length = this.channels.length;
        enableStomping(this.last_cycle_channels);
        foreach ( i, channel; this.channels )
            this.last_cycle_channels[i].copy(this.channels[i]);

        // Get the current list of channels
        this.getCurrentChannels(error);

        // Detect removed channels
        foreach ( old_channel; this.last_cycle_channels )
        {
            // make const view of channels to make badly ported `contains`
            // API happy
            Const!(cstring[]) cchannels = this.channels;
            if ( !cchannels.contains(old_channel) )
            {
                log.info("Detected removed channel '{}'", old_channel);
                this.stats.channelRemoved(old_channel);
            }
        }

        return this.channels;
    }


    /***************************************************************************

        Connects to the dht node and queries the list of channels it contains.

        Params:
            error = set to true if an error occurs while getting the list of
                channels

        Returns:
            list of dht channels

    ***************************************************************************/

    private Const!(mstring[]) getCurrentChannels ( ref bool error )
    {
        log.info("Getting list of channels");
        scope ( exit ) log.info("Got list of channels: {}", this.channels);

        void get_dg ( DhtClient.RequestContext, in cstring addr, ushort port,
            in cstring channel )
        {
            if ( channel.length )
            {
                log.trace("GetChannels: {}:{}, '{}'", addr, port, channel);
                // make const view of channels to make badly ported `contains`
                // API happy
                Const!(cstring[]) cchannels = this.channels;
                if ( !cchannels.contains(channel) )
                {
                    this.channels.appendCopy(channel);
                }
            }
        }

        void notifier ( DhtClient.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                log.error("DhtClient error during GetChannels: {}",
                    info.message(this.dht.msg_buf));
                error = true;
            }
        }

        this.channels.length = 0;
        enableStomping(this.channels);
        this.dht.perform(this,
            this.dht.getChannels(&get_dg, &notifier));

        return this.channels;
    }


    /***************************************************************************

        Dumps the specified channel to disk.

        Params:
            channel = name of the channel to dump
            error = set to true if an error occurs while dumping

    ***************************************************************************/

    private void dumpChannel ( cstring channel, ref bool error )
    {
        ulong records, bytes;

        void get_dg ( DhtClient.RequestContext, in cstring key,
            in cstring value )
        {
            if ( key.length && value.length )
            {
                records++;
                // bytes of key, value, and length specifiers of each
                bytes += key.length + value.length + (size_t.sizeof * 2);

                this.stats.dumpedRecord(key, value);

                this.file.write(key, value);
            }
        }

        void notifier ( DhtClient.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                log.error("DhtClient error during GetAll: {}",
                    info.message(this.dht.msg_buf));
                error = true;
            }
        }

        StopWatch time;
        time.start;

        // Dump channel to file
        try
        {
            buildFilePath(this.root, this.path, channel).cat(".");
            this.file.open(this.path.toString());
            scope ( exit ) this.file.close();

            this.dht.perform(this,
                this.dht.getAll(channel, &get_dg, &notifier));

            this.finalizeChannel(this.file.path, channel, records, bytes, error,
                time.microsec);
        }
        catch ( Exception e )
        {
            log.error("Failed to dump channel to file '{}': {} @ {} : {}",
                this.file.path, getMsg(e), e.file, e.line);
        }
    }


    /***************************************************************************

        Rotates dump file and cleans up intermediary file.

        Params:
            filepath = file which channel was dumped to
            channel = name of the channel which was dumped
            records = number of records dumped
            bytes = number of bytes dumped
            error = true if an error occurred while dumping
            dump_microsec = time taken to dump the channel, in microseconds

    ***************************************************************************/

    private void finalizeChannel ( cstring filepath, cstring channel,
        ulong records, ulong bytes, bool error, ulong dump_microsec )
    {
        if ( error )
        {
            // Delete partial 'channel.dumping' file
            log.warn("Removing partial dump file '{}'", filepath);
            this.path.set(filepath);
            this.path.remove();
        }
        else
        {
            // Atomically move 'channel.dumping' -> 'channel'
            rotateDumpFile(filepath, channel, this.root, this.path,
                this.swap_path);
        }

        log.info("Finished dumping '{}', {} records, {} bytes, {}s{}", channel,
            records, bytes, dump_microsec / 1_000_000f,
            error ? " [error]" : "");

        this.stats.dumpedChannel(channel, records, bytes);
    }


    /***************************************************************************

        Before the first dump, waits a randomly determined amount of time.
        This is to ensure that, in the situation when multiple instances of
        this tool are started simultaneously, they will not all start
        dumping at the same time, in order to minimise impact on the dht.

    ***************************************************************************/

    private void initialWait ( )
    {
        // Set initial wait time randomly (up to dump_period), to ensure that
        // all nodes are not dumping simultaneously.
        scope rand = new Random;
        uint random_wait;
        rand(random_wait);
        auto wait = random_wait % this.dump_config.period_s;

        log.info("Performing initial dump in {}s (randomized)", wait);
        this.timer.wait(wait);
    }


    /***************************************************************************

        After dumping, waits for the remaining time specified in the config.
        If the remaining time is less than the configured minimum wait time,
        then that period is waited instead.

        Params:
            microsec_active = the time (in microseconds) that the dump
                procedure took. This is subtracted from the configured
                period to calculate the wait time
            error = indicates whether a dht error occurred during the last
                dump cycle

    ***************************************************************************/

    private void wait ( ulong microsec_active, bool error )
    {
        double wait;
        if ( error )
        {
            wait = this.dump_config.retry_wait_s;
            log.warn("Dump not completed successfully. Retrying in {}s", wait);
        }
        else
        {
            double sec_active = microsec_active / 1_000_000f;
            wait = this.dump_config.period_s - sec_active;
            if ( wait < this.dump_config.min_wait_s )
            {
                log.warn("Calculated wait time too short -- either the "
                    "channel dump took an unusually long time, or the "
                    "dump period is set too low in config.ini.");
                wait = this.dump_config.min_wait_s;
            }

            auto restart_time =
                WallClock.now() + TimeSpan().fromSeconds(cast(long)wait);
            log.info("Finished dumping channels, took {}s, dumped {}, "
                "sleeping for {}s (next cycle scheduled at {})", sec_active,
                BitGrouping.format(this.stats.total_bytes, this.bytes_buf, "b"),
                wait, restart_time);
        }

        this.timer.wait(wait);
    }
}

