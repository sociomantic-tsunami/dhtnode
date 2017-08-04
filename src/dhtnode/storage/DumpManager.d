/*******************************************************************************

    Memory dumps manager

    This module manages all the dumping/loading related operations for channels.
    It handles opening files, doing the actual dumping, renaming, backing up,
    etc.

    copyright:
        Copyright (c) 2013-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.storage.DumpManager;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dhtnode.storage.StorageEngine;
import dhtnode.storage.DumpFile;

import Hash = swarm.util.Hash;

import dhtproto.client.legacy.DhtConst;

import ocean.core.Array : copy, startsWith;

import ocean.io.FilePath;

import ocean.util.log.StaticTrace;

import ocean.time.StopWatch;

import ocean.util.log.Log;

import core.memory;

/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dhtnode.storage.DumpManager");
}



/*******************************************************************************

    Dump manager class.

    This class takes care of all file operations for the channels (that is:
    loading, dumping, deleting), including opening the files that channels
    should use to dump and load. The manager is careful to maintain the
    integrity of dump files, such that a partially written file will never exist
    with the standard file suffix (they are written to remporary files first).

*******************************************************************************/

public class DumpManager
{
    /***************************************************************************

        Struct wrapping out-of-range records handling options.

    ***************************************************************************/

    public struct OutOfRangeHandling
    {
        /// Enum defining behaviour on encountering an out-of-range record.
        public enum Mode
        {
            Load,   // out-of-range records loaded (logged as trace)
            Fatal,  // quit on finding an out-of-range record (logged as fatal)
            Ignore  // out-of-range records not loaded (logged as warn)
        }

        /// Desried behaviour on encountering an out-of-range record.
        public Mode mode;

        /// If this many records have been loaded from a channel file and all
        /// were out-of-range, then abort. If 0, this behaviour is disabled.
        /// (This is to safeguard against the case where all records are
        /// out-of-range.)
        public uint abort_after_all_out_of_range = 10_000;
    }


    /***************************************************************************

        Callback type used to create a new storage engine instance.

        Params:
            id = name of the channel than needs to be created

    ***************************************************************************/

    public alias StorageEngine delegate ( cstring id ) NewChannelCb;


    /***************************************************************************

        Output buffered direct I/O file, used to dump the channels.

    ***************************************************************************/

    private ChannelDumper output;


    /***************************************************************************

        Input buffered direct I/O file, used to load the channel dumps.

    ***************************************************************************/

    private ChannelLoader input;


    /***************************************************************************

        File paths, re-used for various file operations

    ***************************************************************************/

    private FilePath path;

    private FilePath dst_path;


    /***************************************************************************

        Root directory used to look for files and write dump files.

    ***************************************************************************/

    private FilePath root_dir;


    /***************************************************************************

        Delete directory, inside root directory, where deleted dump files are
        moved to.

    ***************************************************************************/

    private FilePath delete_dir;


    /***************************************************************************

       Strings used for db iteration during load of a dump file.

    ***************************************************************************/

    private mstring load_key, load_value;


    /***************************************************************************

        Determines how out-of-range records (i.e. those whose keys are not in
        the range of hashes supported by the node) are handled (see enum, above)

    ***************************************************************************/

    private OutOfRangeHandling out_of_range_handling;


    /***************************************************************************

        Constructor.

        Params:
            root_dir = root directory used to look for files and write dumps.
            out_of_range_handling = determines how out-of-range records (i.e.
                those whose keys are not in the range of hashes supported by the
                node) are handled (see enum, above)
            disable_direct_io = determines if regular buffered I/O (true) or
                direct I/O is used (false). Regular I/O is only useful for
                testing, because direct I/O imposes some restrictions over the
                type of filesystem that can be used.

    ***************************************************************************/

    public this ( FilePath root_dir, OutOfRangeHandling out_of_range_handling,
        bool disable_direct_io )
    {
        this.root_dir = new FilePath(root_dir.toString());
        this.delete_dir = new FilePath(root_dir.toString());
        this.delete_dir.append("deleted");

        // ensure 'deleted' folder exists
        if ( !this.delete_dir.exists )
        {
            this.delete_dir.create();
        }

        this.path = new FilePath;
        this.dst_path = new FilePath;

        auto buffer = cast(ubyte[]) GC.malloc(IOBufferSize)[0 .. IOBufferSize];
        this.output = new ChannelDumper(buffer, NewFileSuffix,
                disable_direct_io);
        this.input = new ChannelLoader(buffer, disable_direct_io);

        this.out_of_range_handling = out_of_range_handling;
    }


    /***************************************************************************

        Dump a channel in an "atomic" way.

        Dump a channel with name id using the dump_channel callback to do the
        actual dumping. The dump is performed in a temporary file and only
        renamed to the standard dump file name if the dump finishes
        successfully.

        Params:
            storage = storage engine instance to dump

        See_Also:
            rotateDumpFile() for details on the rotation algorithm.

    ***************************************************************************/

    public void dump ( StorageEngine storage )
    {
        // Make the dump and close the file after leaving this scope
        {
            buildFilePath(this.root_dir, this.path, storage.id).cat(".");
            this.output.open(this.path.toString());
            scope (exit) this.output.close();

            this.dumpChannel(storage, this.output);
        }

        // Atomically move dump.new -> dump
        rotateDumpFile(this.output.path, storage.id, this.root_dir, this.path,
            this.dst_path);

        log.info("Finished channel dump, {} bytes written",
            buildFilePath(this.root_dir, this.path, storage.id).fileSize());
    }


    /***************************************************************************

        Writes the contents of a storage engine to a file.

        Params:
            storage = StorageEngine to dump
            output = file where to write the channel dump

    ***************************************************************************/

    private void dumpChannel ( StorageEngine storage, ChannelDumper output )
    {
        log.info("Dumping channel '{}' to disk", storage.id);

        scope progress_manager = new ProgressManager("Dumped channel",
                storage.id, storage.num_records);

        // Write records
        foreach ( key, value; storage )
        {
            // TODO: handle case where out of disk space
            output.write(key, value);
            progress_manager.progress(1);
        }

        log.info("Finished dumping channel '{}' to disk, took {}s, "
            "wrote {} records, {} records in channel",
            storage.id, progress_manager.elapsed, progress_manager.current,
            storage.num_records);
    }


    /***************************************************************************

        Load channel dump files found in this.root_dir.

        We look for dump files in the directory and load the channel when
        a valid file is found. Other files in the data directory trigger a
        warning message (a special warning for partially written dump files).

        Params:
            new_channel = callback to use to create new channels

    ***************************************************************************/

    public void loadChannels ( NewChannelCb new_channel )
    {
        foreach ( info; this.root_dir )
        {
            this.path.set(this.root_dir);
            this.path.append(info.name);

            if ( info.folder )
            {
                // Having the delete_dir there is, of course, fine
                if ( this.path == this.delete_dir )
                    continue;

                log.warn("Ignoring subdirectory '{}' in data directory {}",
                        info.name, this.root_dir.toString);
                continue;
            }

            if ( this.path.suffix() == DumpFileSuffix )
            {
                // We don't reuse this.path for the complete path to avoid
                // conflicts between buffers
                buildFilePath(this.root_dir, this.dst_path, this.path.name);

                this.input.open(this.dst_path.toString());
                scope (exit) this.input.close();

                if ( !this.input.supported_file_format_version )
                {
                    log.warn("{}: Dump file with unsupported version ({}) found "
                        "while scanning directory '{}'. Ignoring file.",
                        this.path.file, this.input.file_format_version,
                        this.root_dir.toString);
                    continue;
                }

                auto channel = new_channel(this.dst_path.name.dup);
                auto dht_channel = cast(StorageEngine)channel;
                assert(dht_channel);

                this.loadChannel(dht_channel, this.input, this.out_of_range_handling);

            }
            else if ( this.path.suffix() == NewFileSuffix )
            {
                log.warn("{}: Unfinished dump file found while scanning "
                        "directory '{}', the program was probably "
                        "restarted uncleanly and data might be old",
                        this.path.file, this.root_dir.toString);
            }
            else
            {
                log.warn("{}: Ignoring file while scanning directory '{}' "
                        "(no '{}' suffix)", this.path.file,
                        this.root_dir.toString, DumpFileSuffix);
            }
        }
    }


    /***************************************************************************

        Loads data from a previously dumped image from a file.

        Params:
            storage = channel storage to load the dump to
            input = file from where to read the channel dump
            out_of_range_handling = out-of-range record handling mode

        Throws:
            if out_of_range_handling == Fatal and an out-of-range record is
            encountered while loading the input file

    ***************************************************************************/

    static private void loadChannel ( StorageEngine storage,
        ChannelLoaderBase input, OutOfRangeHandling out_of_range_handling )
    {
        log.info("Loading channel '{}' from disk", storage.id);

        scope progress_manager = new ProgressManager("Loaded channel",
                    storage.id, input.length);

        ulong records_read, invalid, out_of_range, too_big, empty;
        foreach ( k, v; input )
        {
            records_read++;

            progress_manager.progress(k.length + v.length + (size_t.sizeof * 2));

            loadRecord(storage, k, v, out_of_range_handling,
                out_of_range, invalid, too_big, empty);

            if ( out_of_range_handling.abort_after_all_out_of_range > 0 &&
                records_read == out_of_range && out_of_range >
                out_of_range_handling.abort_after_all_out_of_range )
            {
                log.fatal("All records appear to be out-of-range. Aborting.");
                throw new Exception(
                    "All records appear to be out-of-range. Aborting.");
            }
        }

        void reportBadRecordCount ( ulong bad, cstring desc )
        {
            if ( bad )
            {
                auto percent_bad =
                    (cast(float)bad / cast(float)records_read) * 100.0;
                log.warn("Found {} {} ({}%) in channel '{}'", bad, desc,
                    percent_bad, storage.id);
            }
        }

        reportBadRecordCount(out_of_range, "out-of-range keys");
        reportBadRecordCount(invalid, "invalid keys");
        reportBadRecordCount(too_big, "too large values");
        reportBadRecordCount(empty, "empty values");

        log.info("Finished loading channel '{}' from disk, took {}s, "
            "read {} bytes (file size including padding is {} bytes), "
            "{} records in channel", storage.id, progress_manager.elapsed,
            progress_manager.current, progress_manager.maximum,
            storage.num_records);
    }


    /***************************************************************************

        Loads a record into the specified storage channel. Checks whether the
        record's key is valid and within the hash range of the storage engine.

        Params:
            storage = channel storage to load the record into
            key = record key
            val = record value
            out_of_range_handling = out-of-range record handling mode
            out_of_range = count of out-of-range records; incremented if key is
                outside of the node's hash range
            invalid = count of invalid keys; incremented if key is invalid
            too_big = count of too large records; incremented if value is bigger
                than defined maximum (see DhtConst.RecordSizeLimit)
            empty = count of empty records; incremented if record value has
                length 0

        Throws:
            if out_of_range_handling == Fatal and the record is out-of-range

    ***************************************************************************/

    static private void loadRecord ( StorageEngine storage, cstring key,
        cstring val, OutOfRangeHandling out_of_range_handling,
        ref ulong out_of_range, ref ulong invalid, ref ulong too_big,
        ref ulong empty )
    {
        if ( !Hash.isHash(key) )
        {
            log.error("Encountered invalid non-hexadecimal key in channel '{}': "
                "{} -- ignored", storage.id, key);
            invalid++;
            return;
        }

        if ( val.length == 0 )
        {
            log.warn("Encountered empty record in channel '{}': {} -- discarded",
                storage.id, key);
            empty++;
            return;
        }

        if ( val.length > DhtConst.RecordSizeLimit )
        {
            log.warn("Encountered large record ({} bytes) in channel '{}': "
                "{} -- ignored", val.length, storage.id, key);
            too_big++;
            return;
        }

        if ( storage.responsibleForKey(key) )
        {
            storage.put(key, val);
        }
        else
        {
            with ( OutOfRangeHandling.Mode ) switch ( out_of_range_handling.mode )
            {
                case Load:
                    log.trace("Encountered out-of-range key in channel '{}': "
                        "{} -- loaded", storage.id, key);
                    storage.put(key, val);
                    out_of_range++;
                    return;

                case Fatal:
                    log.fatal("Encountered out-of-range key in channel '{}': "
                    "{} -- rejected", storage.id, key);
                    throw new Exception(
                        cast(istring)("Encountered out-of-range key in channel '"
                            ~ storage.id ~ "': " ~ key)
                    );

                case Ignore:
                    log.warn("Encountered out-of-range key in channel '{}': "
                        "{} -- ignored", storage.id, key);
                    out_of_range++;
                    return;

                default:
                    assert(false);
            }
        }
    }


    /***************************************************************************

        Virtually delete a channel dump file.

        What this method really does is move the old file into the 'deleted'
        sub-folder of the data directory. Files in this folder are not loaded by
        loadChannels().

        Params:
            id = name of the channel to delete

    ***************************************************************************/

    public void deleteChannel ( cstring id )
    {
        buildFilePath(this.root_dir, this.path, id);
        if ( this.path.exists )
        {
            buildFilePath(this.delete_dir, this.dst_path, id);
            // file -> deleted/file
            this.path.rename(this.dst_path);
        }
    }
}



/*******************************************************************************

    Unittest for DumpManager.loadChannel()

*******************************************************************************/

version ( UnitTest )
{
    import ocean.core.Test : test, testThrown;
    import ocean.io.device.MemoryDevice;
    import ocean.core.ExceptionDefinitions : IOException;
    import dhtnode.config.HashRangeConfig;
    import ocean.io.serialize.SimpleStreamSerializer : EofException;

    private class DummyStorageEngine : StorageEngine
    {
        private uint count;

        this ( hash_t min = hash_t.min, hash_t max = hash_t.max )
        {
            super("test", new DhtHashRange(min, max, new HashRangeConfig([])),
                200, null);
        }
        override typeof(this) put ( cstring key, cstring value, bool trigger )
        {
            this.count++;
            return this;
        }
        override ulong num_records ( ) { return this.count; }
    }

    private class DummyChannelLoader : ChannelLoaderBase
    {
        size_t len;
        this ( ubyte[] data )
        {
            this.len = data.length;
            auto mem = new MemoryDevice;
            mem.write(data);
            mem.seek(0);
            super(mem);
        }
        override protected ulong length_ ( ) { return this.len; }
    }


    /***************************************************************************

        Calls DumpManager.loadChannel() with the provided input data.

        Params:
            data = data to load
            out_of_range_handling = behaviour upon finding out-of-range records
            min = min hash of dummy node
            max = max hash of dummy node

        Returns:
            the number of records in the storage engine after loading

    ***************************************************************************/

    ulong testData ( ubyte[] data, DumpManager.OutOfRangeHandling.Mode
        out_of_range_handling_mode = DumpManager.OutOfRangeHandling.Mode.Load,
        hash_t min = hash_t.min, hash_t max = hash_t.max )
    {
        auto storage = new DummyStorageEngine(min, max);
        auto input = new DummyChannelLoader(data);
        input.open();

        DumpManager.OutOfRangeHandling out_of_range_handling;
        out_of_range_handling.mode = out_of_range_handling_mode;
        DumpManager.loadChannel(storage, input, out_of_range_handling);

        return storage.num_records;
    }

}


/*******************************************************************************

    Tests for handling file version and extra bytes at the end of the file.
    (File format version 0 appends an extra 8 bytes of value 0 at the end of the
    file to indicate that there are no more records.)

*******************************************************************************/

unittest
{
    ubyte[] version0 = [0,0,0,0,0,0,0,0]; // version 0 (supported)
    ubyte[] version1 = [1,0,0,0,0,0,0,0]; // version 1 (unsupported)
    ubyte[] data = [
        16,0,0,0,0,0,0,0, // key 1 len
        49,50,51,52,53,54,55,56,49,50,51,52,53,54,55,56, // key 1
        4,0,0,0,0,0,0,0, // value 1 len
        1,2,3,4, // value 1
        16,0,0,0,0,0,0,0, // key 2 len
        49,50,51,52,53,54,55,56,49,50,51,52,53,54,55,56, // key 2
        4,0,0,0,0,0,0,0, // value 2 len
        1,2,3,4, // value 2
        16,0,0,0,0,0,0,0, // key 3 len
        49,50,51,52,53,54,55,56,49,50,51,52,53,54,55,56, // key 3
        4,0,0,0,0,0,0,0, // value 3 len
        1,2,3,4 // value 3
    ];
    ubyte[] extra = [0,0,0,0,0,0,0,0];

    // version 1 file with no extra bytes at end
    testThrown!(Exception)(testData(version1 ~ data));

    // version 1 file with extra bytes at end
    testThrown!(Exception)(testData(version1 ~ data ~ extra));

    // version 0 file with no extra bytes at end
    testThrown!(EofException)(testData(version0 ~ data));

    // version 0 file with extra bytes at end
    test!("==")(testData(version0 ~ data ~ extra), 3);
}


/*******************************************************************************

    Tests for out-of-range record handling.

*******************************************************************************/

unittest
{
    ubyte[] data = [
        0,0,0,0,0,0,0,0, // version number
        16,0,0,0,0,0,0,0, // key 1 len
        48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,49, // key 1 (..001)
        4,0,0,0,0,0,0,0, // value 1 len
        1,2,3,4, // value 1
        16,0,0,0,0,0,0,0, // key 2 len
        48,48,48,48,48,48,48,48,49,48,48,48,48,48,48,48, // key 2 (..010..)
        4,0,0,0,0,0,0,0, // value 2 len
        1,2,3,4, // value 2
        16,0,0,0,0,0,0,0, // key 3 len
        49,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48, // key 3 (100..)
        4,0,0,0,0,0,0,0, // value 3 len
        1,2,3,4, // value 3
        0,0,0,0,0,0,0,0 // EOF
    ];

    // no out-of-range records
    test!("==")(testData(data, DumpManager.OutOfRangeHandling.Mode.Ignore,
        hash_t.min, hash_t.max), 3);

    // load out-of-range records
    test!("==")(testData(data, DumpManager.OutOfRangeHandling.Mode.Load, 0, 1),
        3);

    // ignore out-of-range records
    test!("==")(testData(data, DumpManager.OutOfRangeHandling.Mode.Ignore,
        0, 1), 0);
    test!("==")(testData(data, DumpManager.OutOfRangeHandling.Mode.Ignore,
        0x0000000100000000, hash_t.max), 2);

    // fatal upon out-of-range record
    testThrown!(Exception)(testData(data,
        DumpManager.OutOfRangeHandling.Mode.Fatal, 0, 1));
}



/*******************************************************************************

    Tests for invalid key handling.

*******************************************************************************/

unittest
{
    ubyte[] data_header = [ 0,0,0,0,0,0,0,0 ]; // version number
    ubyte[] data_footer = [ 0,0,0,0,0,0,0,0 ]; // EOF

    // load good record
    ubyte[] good = [
        16,0,0,0,0,0,0,0, // key len
        48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,49, // good key
        4,0,0,0,0,0,0,0, // value len
        1,2,3,4 // value
    ];
    test!("==")(testData(data_header ~ good ~ data_footer), 1);

    // ignore record with short key
    ubyte[] short_key = [
        15,0,0,0,0,0,0,0, // key len
        48,48,48,48,48,48,48,48,48,48,48,48,48,48,49, // short key
        4,0,0,0,0,0,0,0, // value len
        1,2,3,4 // value
    ];
    test!("==")(testData(data_header ~ short_key ~ data_footer), 0);

    // ignore record with long key
    ubyte[] long_key = [
        17,0,0,0,0,0,0,0, // key len
        48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,49, // short key
        4,0,0,0,0,0,0,0, // value len
        1,2,3,4 // value
    ];
    test!("==")(testData(data_header ~ long_key ~ data_footer), 0);

    // ignore record with non-hexadecimal key
    ubyte[] bad_key = [
        16,0,0,0,0,0,0,0, // key len
        47,48,48,48,48,48,48,48,48,48,48,48,48,48,48,49, // bad key
        4,0,0,0,0,0,0,0, // value len
        1,2,3,4 // value
    ];
    test!("==")(testData(data_header ~ bad_key ~ data_footer), 0);
}



/*******************************************************************************

    Tests for over-large record handling.

*******************************************************************************/

unittest
{
    /***************************************************************************

        Params:
            len = bytes of data

        Returns:
            array containing a serialized length and data.

    ***************************************************************************/

    ubyte[] recordValue ( size_t len )
    out ( v )
    {
        assert(v.length == len + size_t.sizeof);
    }
    body
    {
        ubyte[] value;
        value.length = size_t.sizeof + len;
        *(cast(ulong*)value.ptr) = len;
        return value;
    }

    ubyte[] data_header = [ 0,0,0,0,0,0,0,0 ]; // version number
    ubyte[] data_footer = [ 0,0,0,0,0,0,0,0 ]; // EOF
    ubyte[] key = [
        16,0,0,0,0,0,0,0, // key len
        48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,49 // key
    ];

    // load small record
    auto small_val = recordValue(4);
    test!("==")(testData(data_header ~ key ~ small_val ~ data_footer), 1);

    // load maximum sized record
    auto max_val = recordValue(DhtConst.RecordSizeLimit);
    test!("==")(testData(data_header ~ key ~ max_val ~ data_footer), 1);

    // reject over-large record
    auto over_large_val = recordValue(DhtConst.RecordSizeLimit + 1);
    test!("==")(testData(data_header ~ key ~ over_large_val ~ data_footer), 0);
}



/*******************************************************************************

    Tests for empty record handling.

*******************************************************************************/

unittest
{
    ubyte[] data_header = [ 0,0,0,0,0,0,0,0 ]; // version number
    ubyte[] data_footer = [ 0,0,0,0,0,0,0,0 ]; // EOF
    ubyte[] key = [
        16,0,0,0,0,0,0,0, // key len
        48,48,48,48,48,48,48,48,48,48,48,48,48,48,48,49 // key
    ];

    // load normal record
    ubyte[] good = [
        4,0,0,0,0,0,0,0, // value len
        1,2,3,4 // value
    ];
    test!("==")(testData(data_header ~ key ~ good ~ data_footer), 1);

    // discard empty record
    ubyte[] empty = [
        0,0,0,0,0,0,0,0 // value len
    ];
    test!("==")(testData(data_header ~ key ~ empty ~ data_footer), 0);

    // discard empty record mixed among normal records
    test!("==")(testData(data_header
        ~ key ~ good
        ~ key ~ empty
        ~ key ~ good
        ~ data_footer), 2);
}



/*******************************************************************************

    Track the progress of a task and measures its progress.

    Used as a helper class for loading and dumping.

*******************************************************************************/

private scope class ProgressManager
{
    /***************************************************************************

        Stopwatch to use to measure the processing time.

    ***************************************************************************/

    private StopWatch sw;


    /***************************************************************************

        Name of the activity / process we are timing.

    ***************************************************************************/

    private cstring activity;


    /***************************************************************************

        Name of the particular instance of the activity / process.

    ***************************************************************************/

    private cstring name;


    /***************************************************************************

        Value to be considered 100%.

    ***************************************************************************/

    private ulong max;


    /***************************************************************************

        Current value.

    ***************************************************************************/

    private ulong curr;


    /***************************************************************************

        Time of the previous progress display.

    ***************************************************************************/

    private float prev_time;


    /***************************************************************************

        Minimum advance (in seconds) necessary for a progress() to be printed.

    ***************************************************************************/

   private static MinAdvanceSecs = 1.0;


    /***************************************************************************

        Constructor.

        Params:
            activity = name of the activity / process we are timing
            name = name of the particular instance of the activity / process
            max = value to be considered 100%

    ***************************************************************************/

    public this ( cstring activity, cstring name, ulong max )
    {
        this.activity = activity;
        this.name = name;
        this.max = max;
        this.curr = 0;
        this.prev_time = 0;
        this.sw.start;
    }


    /***************************************************************************

        Logs the progress if appropriate.

        This method should be called each time there is a progress in the
        process being timed.

        Params:
            advance = number of units advanced in this progress

    ***************************************************************************/

    public void progress ( ulong advance )
    {
        auto old_value = this.curr;
        this.curr += advance;

        if ( this.elapsed > this.prev_time + MinAdvanceSecs )
        {
            log.trace("{}: {}%", this.name,
                    (cast(float) this.curr / max) * 100.0f);

            this.prev_time = this.elapsed;
        }
    }


    /***************************************************************************

        Destructor. Reports the time it took to process the activity.

    ***************************************************************************/

    ~this ( )
    {
        log.trace("{} '{}' in {}s", this.activity, this.name, this.elapsed);
    }


    /***************************************************************************

        Get the current value of the progress.

    ***************************************************************************/

    public final ulong current ( )
    {
        return this.curr;
    }


    /***************************************************************************

        Get the value that represents 100%.

    ***************************************************************************/

    public final ulong maximum ( )
    {
        return this.max;
    }


    /***************************************************************************

        Return the time elapsed since object construction in seconds.

    ***************************************************************************/

    public float elapsed ( )
    {
        return this.sw.microsec() / 1_000_000.0f;
    }
}

