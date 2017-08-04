/*******************************************************************************

    Stats aggregator and stats.log writer for dump cycle.

    copyright:
        Copyright (c) 2014-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.dhtdump.DumpStats;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import ocean.core.Array : concat;

import ocean.util.log.Stats;

import ocean.io.select.EpollSelectDispatcher;



public class DumpStats
{
    /***************************************************************************

        Alias for the stats logger config class.

    ***************************************************************************/

    public alias StatsLog.Config Config;


    /***************************************************************************

        Stats logging class to write to when log() is called.

    ***************************************************************************/

    private StatsLog stats_log;

    /***************************************************************************

        Struct wrapping the set of stats to be recorded about a dump cycle.

    ***************************************************************************/

    private struct CycleStats
    {
        ulong last_time_ms;
    }


    /***************************************************************************

        Total data written since the last log update.

    ***************************************************************************/

    private CycleStats cycle_stats;


    /***************************************************************************

        Struct wrapping the set of stats to be recorded about an I/O process.

    ***************************************************************************/

    private struct IOStats
    {
        ulong records_written;
        ulong bytes_written;
    }


    /***************************************************************************

        Total data written since the last log update. Cleared after updating.

    ***************************************************************************/

    private IOStats io_stats;


    /***************************************************************************

        Data written per channel. Only updated *after* a channel has been
        completely dumped. Never cleared but elements may be removed (see
        channelRemoved()).

    ***************************************************************************/

    private IOStats[cstring] channel_stats;


    /***************************************************************************

        Constructor. Registers an update timer with epoll which writes the stats
        to the log periodically.

        Params:
            stats_log = stats logger to write to when log() is called

    ***************************************************************************/

    public this ( StatsLog stats_log )
    {
        this.stats_log = stats_log;
    }


    /***************************************************************************

        Should be called when a record has been dumped. Updates the stats
        counters with the amount of data written to disk for this record.

        Params:
            key = key of record dumped
            value = value of record dumped

    ***************************************************************************/

    public void dumpedRecord ( cstring key, cstring value )
    {
        this.io_stats.records_written++;
        // bytes of key, value, and length specifiers of each
        this.io_stats.bytes_written += key.length + value.length
            + (size_t.sizeof * 2);
    }


    /***************************************************************************

        Should be called when a channel has been dumped. Updates the stats
        counters.

        Params:
            channel = name of channel which was dumped
            records = total number of records in channel
            bytes = total number of bytes in channel

    ***************************************************************************/

    public void dumpedChannel ( cstring channel, ulong records, ulong bytes )
    {
        if ( !(channel in this.channel_stats) )
        {
            this.channel_stats[channel] = IOStats();
        }

        this.channel_stats[channel].records_written = records;
        this.channel_stats[channel].bytes_written = bytes;
    }


    /***************************************************************************

        Should be called when a channel has been removed. Stats for the channel
        will no longer be tracked or output.

        (Note that, as we're using a standard AA, this operation will cause a
        map element to be discarded and not reused. Removing a channel happens
        so rarely, though, that this will not cause excessive GC activity, under
        normal use.)

        Params:
            channel = name of channel which was removed

    ***************************************************************************/

    public void channelRemoved ( char[] channel )
    {
        this.channel_stats.remove(channel);
    }


    /***************************************************************************

        Should be called when a complete dump cycle has finished. Updates the
        stats counters.

        Params:
            millisec = time in ms taken to complete the dump cycle

    ***************************************************************************/

    public void dumpedAll ( ulong millisec )
    {
        this.cycle_stats.last_time_ms = millisec;
    }


    /***************************************************************************

        Returns:
            the total number of bytes written to all channels during the last
            cycle

    ***************************************************************************/

    public ulong total_bytes ( )
    {
        ulong sum;
        foreach ( channel; this.channel_stats )
        {
            sum += channel.bytes_written;
        }
        return sum;
    }


    /***************************************************************************

        Returns:
            the total number of records written to all channels during the last
            cycle

    ***************************************************************************/

    public ulong total_records ( )
    {
        ulong sum;
        foreach ( channel; this.channel_stats )
        {
            sum += channel.records_written;
        }
        return sum;
    }


    /***************************************************************************

        Writes the stats to the logger provided to the constructor.

    ***************************************************************************/

    public void log ( )
    {
        this.stats_log.add(this.io_stats);
        this.stats_log.add(this.cycle_stats);

        foreach ( channel, stats; this.channel_stats )
        {
            this.stats_log.addObject!("channel")(channel, stats);
        }

        this.io_stats = this.io_stats.init;

        this.stats_log.flush();
    }
}

