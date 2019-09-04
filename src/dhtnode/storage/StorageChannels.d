/*******************************************************************************

    In-memory hashtable storage engine

    copyright:
        Copyright (c) 2011-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.storage.StorageChannels;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import swarm.node.storage.model.IStorageChannels;

import dhtnode.storage.StorageEngine;

import ocean.util.log.Logger;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dhtnode.storage.MemoryStorageChannels");
}



/*******************************************************************************

    Memory storage channels class

*******************************************************************************/

public class StorageChannels : IStorageChannelsTemplate!(StorageEngine)
{
    import dhtnode.node.DhtHashRange;
    import dhtnode.storage.StorageEngineStepIterator;

    import dhtnode.storage.StorageEngine : StorageEngine;
    import dhtnode.storage.DumpManager : DumpManager;

    import ocean.io.FilePath;

    import Hash = swarm.util.Hash;

    import ocean.core.Verify;
    import ocean.sys.Environment;
    import ocean.time.StopWatch;


    /***************************************************************************

        Public alias of enum type.

    ***************************************************************************/

    public alias DumpManager.OutOfRangeHandling OutOfRangeHandling;


    /***************************************************************************

        Storage data directory (copied in constructor)

    ***************************************************************************/

    private FilePath dir;


    /***************************************************************************

        Minimum and maximum record hashes supported by node

    ***************************************************************************/

    private DhtHashRange hash_range_;


    /***************************************************************************

        Estimated number of buckets in map -- passed to tokyocabinet when
        creating database instances.

    ***************************************************************************/

    private uint bnum;


    /// Batch size used by legacy compressed batch requests (e.g. GetAll).
    private size_t batch_size_;


    /***************************************************************************

        State of storage channels.

        The ChannelsScan state is set during the loadChannels() method, and is
        checked by the MemoryStorage constructor to make sure that dumped files
        are only loaded during node startup, and not when a new channel is
        created when the node is running.

        The ShuttingDown state is set by the shutdown_ method, and is checked by
        the dump() method of MemoryStorage.

    ***************************************************************************/

    private enum State
    {
        Init,           // Invalid
        ChannelsScan,   // Scanning for / loading dumped channels
        Running,        // Normal running state
        ShuttingDown    // Shutting down / dumping channels
    }

    private State state;


    /***************************************************************************

        Memory storage dump file manager.

    ***************************************************************************/

    private DumpManager dump_manager;


    /***************************************************************************

        Constructor. If the specified data directory exists, it is scanned for
        dumped memory channels, which are loaded. Otherwise the data directory
        is created.

        Params:
            dir = data directory for dumped memory channels
            size_limit = maximum number of bytes allowed in the node (0 = no
                limit)
            hash_range = hash range for which this node is responsible
            bnum = estimated number of buckets in map (passed to tokyocabinet
                "ctor")
            out_of_range_handling = determines how out-of-range records (i.e.
                those whose keys are not in the range of hashes supported by the
                node) are handled (see DumpManager)
            disable_direct_io = determines if regular buffered I/O (true) or
                direct I/O is used (false). Regular I/O is only useful for
                testing, because direct I/O imposes some restrictions over the
                type of filesystem that can be used.
            batch_size = batch size used by legacy compressed batch requests
                (e.g. GetAll).

    ***************************************************************************/

    public this ( cstring dir, ulong size_limit, DhtHashRange hash_range,
        uint bnum, OutOfRangeHandling out_of_range_handling,
        bool disable_direct_io, size_t batch_size )
    {
        super(size_limit);

        this.dir = this.getWorkingPath(dir);

        if ( !this.dir.exists )
        {
            this.createWorkingDir();
        }

        this.hash_range_ = hash_range;
        this.bnum = bnum;
        this.batch_size_ = batch_size;

        this.dump_manager = new DumpManager(this.dir, out_of_range_handling,
            disable_direct_io);

        this.loadChannels();
    }


    /***************************************************************************

        Creates a new instance of an iterator for this storage engine.

        Returns:
            new iterator

    ***************************************************************************/

    public StorageEngineStepIterator newIterator ( )
    {
        return new StorageEngineStepIterator;
    }


    /***************************************************************************

        Returns:
             string identifying the type of the storage engine

    ***************************************************************************/

    override public cstring type ( )
    {
        return "Memory";
    }


    /***************************************************************************

        Returns:
             batch size used by legacy compressed batch requests (e.g. GetAll)

    ***************************************************************************/

    public size_t batch_size ( )
    {
        return this.batch_size_;
    }


    /***************************************************************************

        Checks whether the specified key string (expected to be a hex number) is
        within the hash range of this storage engine.

        Params:
            key = record key

        Returns:
            true if the key is within the storage engine's hash range

    ***************************************************************************/

    public bool responsibleForKey ( cstring key )
    {
        auto hash = Hash.straightToHash(key);
        return Hash.isWithinNodeResponsibility(hash, this.hash_range_.range.min,
            this.hash_range_.range.max);
    }


    /***************************************************************************

        Getter for the hash-range object.

        Returns:
            the hash-range object referenced in the storage channels

    ***************************************************************************/

    public DhtHashRange hash_range ( )
    {
        return this.hash_range_;
    }


    /***************************************************************************

        Returns:
             string identifying the type of the storage engine

    ***************************************************************************/

    protected override void shutdown_ ( )
    {
        this.state = State.ShuttingDown;

        log.info("Closing memory channels");

        StopWatch sw;
        sw.start();
        foreach ( channel; this )
        {
            auto dht_channel = cast(StorageEngine)channel;
            verify(dht_channel !is null);
            this.dump_manager.dump(dht_channel);
        }

        auto seconds = (cast(double)sw.microsec) / 1_000_000.0;
        log.info("Finished closing memory channels, took {}s", seconds);
    }


    /***************************************************************************

        Creates a new StorageEngine instance with the specified id.

        Params:
            id = channel id

        Returns:
            new StorageEngine instance

    ***************************************************************************/

    protected override StorageEngine create_ ( cstring id )
    {
        return new StorageEngine(id, this.hash_range_, this.bnum,
                &this.dump_manager.deleteChannel);
    }


    /***************************************************************************

        Searches this.dir for files with DumpFileSuffix suffix and creates and
        load the channels with the contents of the dump files.

    ***************************************************************************/

    private void loadChannels ( )
    {
        log.info("Scanning {} for memory files", this.dir.toString);

        this.state = State.ChannelsScan;
        scope ( exit ) this.state = State.Running;

        StopWatch sw;
        sw.start();
        this.dump_manager.loadChannels(&this.create);

        auto seconds = (cast(double)sw.microsec) / 1_000_000.0;
        log.info("Finished scanning {} for memory files, took {}s",
            this.dir.toString, seconds);
    }


    /***************************************************************************

        Creates a FilePath instance set to the absolute path of dir, if dir is
        not null, or to the current working directory of the environment
        otherwise.

        Params:
            dir = directory string; null indicates that the current working
                  directory of the environment should be used

        Returns:
            FilePath instance holding path

    ***************************************************************************/

    private FilePath getWorkingPath ( cstring dir )
    {
        FilePath path = new FilePath;

        if ( dir )
        {
            path.set(dir);

            if ( !path.isAbsolute() )
            {
                path.prepend(Environment.cwd());
            }
        }
        else
        {
            path.set(Environment.cwd());
        }

        return path;
    }


    /***************************************************************************

        Creates data directory.

    ***************************************************************************/

    private void createWorkingDir ( )
    {
        try
        {
            this.dir.createFolder();
        }
        catch (Exception e)
        {
            e.msg = typeof(this).stringof ~ ": Failed creating directory: " ~ e.msg;

            throw e;
        }
    }
}

