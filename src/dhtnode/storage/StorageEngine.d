/*******************************************************************************

    Memory Channel Storage Engine

    This module implements the IStorageEngine for a memory channel using
    Tokyo Cabinet as the real storage engine.

    copyright:
        Copyright (c) 2013-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.storage.StorageEngine;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dhtproto.client.legacy.DhtConst;

import swarm.node.storage.model.IStorageEngine;
import swarm.node.storage.listeners.Listeners;

import dhtnode.storage.StorageEngineStepIterator;

import ocean.util.log.Logger;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dhtnode.storage.MemoryStorage");
}



/***************************************************************************

    Memory storage engine

***************************************************************************/

public class StorageEngine : IStorageEngine
{
    import dhtnode.node.DhtHashRange;
    import dhtnode.storage.tokyocabinet.c.tcmdb :
        TCMDB,
        tcmdbnew, tcmdbnew2, tcmdbvanish, tcmdbdel,
        tcmdbput, tcmdbputkeep, tcmdbputcat,
        tcmdbget, tcmdbforeach,
        tcmdbout, tcmdbrnum, tcmdbmsiz, tcmdbvsiz,
        tcmdbiterinit, tcmdbiterinit2, tcmdbiternext;

    import Hash = swarm.util.Hash;

    import ocean.core.Verify;
    import ocean.core.TypeConvert;
    import core.stdc.stdlib : free;

    /***********************************************************************

        Callback type used to delete channel files when the channel is removed.

        Params:
            id = name of the channel to remove

    ***********************************************************************/

    public alias void delegate ( cstring id ) DeleteChannelCb;

    private DeleteChannelCb delete_channel;


    /***************************************************************************

        Set of listeners waiting for data on this storage channel. When data
        arrives (or a flush / finish signal for the channel), all registered
        listeners are notified.

    ***************************************************************************/

    protected alias IListeners!(hash_t) Listeners;

    protected Listeners listeners;


    /***************************************************************************

        Alias for a listener.

    ***************************************************************************/

    public alias Listeners.Listener IListener;


    /***************************************************************************

        Tokyo cabinet database instance

    ***************************************************************************/

    private TCMDB* db;


    /***************************************************************************

        Minimum and maximum record hashes supported by node

    ***************************************************************************/

    private DhtHashRange hash_range;


    /***********************************************************************

        Constructor.

        Params:
            id = identifier string for this instance
            hash_range = hash range for which this node is responsible
            bnum = memory storage channels bnum value
            delete_channel = callback used to delete channel files when the
                    channel is removed

    ***********************************************************************/

    public this ( cstring id, DhtHashRange hash_range, uint bnum,
        DeleteChannelCb delete_channel )
    {
        super(id);

        this.hash_range = hash_range;
        this.delete_channel = delete_channel;

        this.listeners = new Listeners;

        if ( bnum == 0 )
        {
            this.db = tcmdbnew();
        }
        else
        {
            this.db = tcmdbnew2(bnum);
        }
    }


    /***********************************************************************

        Puts a record into the database.

        Params:
            key        = record key
            value      = record value
            trigger_listeners = if true, any listeners registered for this
                channel will be notified of the update to this record

        Returns:
            this instance

    ***********************************************************************/

    public typeof(this) put ( cstring key, cstring value,
        bool trigger_listeners = true )
    {
        auto hash = Hash.straightToHash(key);
        return this.put(hash, value, trigger_listeners);
    }


    /***************************************************************************

        Ditto

    ***************************************************************************/

    public typeof(this) put ( hash_t key, cstring value,
        bool trigger_listeners = true )
    {
        tcmdbput(this.db, &key, castFrom!(size_t).to!(int)(key.sizeof),
            value.ptr, castFrom!(size_t).to!(int)(value.length));

        if ( trigger_listeners )
            this.listeners.trigger(Listeners.Listener.Code.DataReady, key);

        return this;
    }


    /***********************************************************************

       Get record

       Params:
           key   = key to lookup
            value_buffer = buffer to receive record value output. The length of
                this buffer is never decreased, only increased (if necessary).
                This is an optimization, as this method is called extremely
                frequently and array length resetting is not free (especially in
                D2 builds, where assumeSafeAppend must be called).
            value = record value output (slice of value_buffer)

       Returns:
           this instance

    ***********************************************************************/

    public typeof(this) get ( cstring key, ref mstring value_buffer,
        out mstring value )
    {
        auto hash = Hash.straightToHash(key);
        return this.get(hash, value_buffer, value);
    }


    /***************************************************************************

        Ditto

    ***************************************************************************/

    public typeof(this) get ( hash_t key, ref mstring value_buffer,
        out mstring value )
    {
        int len;
        if ( auto value_ = cast(void*)tcmdbget(this.db, &key,
            castFrom!(size_t).to!(int)(key.sizeof), &len) )
        {
            if ( value_buffer.length < len )
                value_buffer.length = len;

            value_buffer[0..len] = (cast(char*)value_)[0..len];
            value = value_buffer[0..len];

            free(value_);
        }

        return this;
    }


    /***************************************************************************

        Gets the value associated with the specified key and passes it to the
        provided delegate. If the record does not exist, the delegate is not
        called.

        Note: passing the value to the caller via a delegate, like this, is an
        optimisation: it requires one less buffer copy.

        Params:
            key = key to look up
            value_dg = delegate to pass the record value to, if it exists

        Returns:
            this instance

    ***************************************************************************/

    public typeof(this) get ( cstring key, void delegate ( cstring ) value_dg )
    {
        auto hash = Hash.straightToHash(key);
        return this.get(hash, value_dg);
    }


    /***************************************************************************

        Ditto.

    ***************************************************************************/

    public typeof(this) get ( hash_t key, void delegate ( cstring ) value_dg )
    {
        int len;
        void* value = cast(void*)tcmdbget(this.db, &key,
            castFrom!(size_t).to!(int)(key.sizeof), &len);

        if ( value !is null )
        {
            auto value_str = (cast(char*)value)[0..len];
            value_dg(value_str);
            free(value);
        }

        return this;
    }


    /***********************************************************************

       Get record's size

       Params:
           key   = key to lookup

       Returns:
           size of record in bytes (0 if it doesn't exist)

    ***********************************************************************/

    public size_t getSize ( cstring key )
    {
        auto hash = Hash.straightToHash(key);
        return this.getSize(hash);
    }


    /***************************************************************************

        Ditto.

    ***************************************************************************/

    public size_t getSize ( hash_t key )
    {
        auto s = tcmdbvsiz(this.db,
            &key, castFrom!(size_t).to!(int)(key.sizeof));
        return s < 0 ? 0 : s;
    }


    /***********************************************************************

        Tells whether a record exists

         Params:
            key = record key

        Returns:
             true if record exists or false itherwise

   ************************************************************************/

    public bool exists ( cstring key )
    {
        auto hash = Hash.straightToHash(key);
        return this.exists(hash);
    }


    /***************************************************************************

        Ditto.

    ***************************************************************************/

    public bool exists ( hash_t key )
    {
        int size;
        size = tcmdbvsiz(this.db, &key, castFrom!(size_t).to!(int)(key.sizeof));
        return size >= 0;
    }


    /***********************************************************************

        Remove record

        Params:
            key = key of record to remove

        Returns:
            this instance

    ***********************************************************************/

    public typeof(this) remove ( cstring key )
    {
        auto hash = Hash.straightToHash(key);
        return this.remove(hash);
    }


    /***************************************************************************

        Ditto.

    ***************************************************************************/

    public typeof(this) remove ( hash_t key )
    {
        tcmdbout(this.db, &key, castFrom!(size_t).to!(int)(key.sizeof));
        this.listeners.trigger(Listeners.Listener.Code.Deletion, key);
        return this;
    }


    /***************************************************************************

        Checks whether the specified key string (expected to be a hex number) is
        within the hash range of this storage engine.

        Params:
            key = record key

        Returns:
            true if the key is within the storage engine's hash range

        TODO: duplicate of same function in MemoryStorageChannels

    ***************************************************************************/

    public bool responsibleForKey ( cstring key )
    {
        auto hash = Hash.straightToHash(key);
        return Hash.isWithinNodeResponsibility(hash, this.hash_range.range.min,
            this.hash_range.range.max);
    }


    /**************************************************************************

        opApply delegate. If the delegate returns <> 0, the iteration will be
        aborted.

    ***************************************************************************/

    public alias int delegate ( ref hash_t key, ref char[] value ) IterDg;


    /***************************************************************************

        Context for db iteration (just stores a user-provided delegate)

    ***************************************************************************/

    private struct IterContext
    {
        /***********************************************************************

            User-provided opApply delegate to receive each record's key and
            value

        ***********************************************************************/

        IterDg dg;


        /***********************************************************************

            Return value of last call of the delegate. Required by opApply.

        ***********************************************************************/

        int ret;
    }


    /***************************************************************************

        TokyoCabinet-compatible iteration function. The opaque void* (the last
        parameter) contains the iteration context (see iterate(), above), which
        in turn contains the user-provided iteration delegate.

        Params:
            key_ptr = pointer to buffer containing record key
            key_len = length of buffer containing record key
            val_ptr = pointer to buffer containing record value
            val_len = length of buffer containing record value
            context_ = opaque context pointer (set in opApply(), above)

        Returns:
            true to continue iterating, false to break

    ***************************************************************************/

    extern ( C ) private static bool db_iter ( void* key_ptr, int key_len,
        void* val_ptr, int val_len, void* context_ )
    {
        auto key = *(cast(hash_t*)key_ptr);
        auto val = (cast(char*)val_ptr)[0..val_len];
        auto context = cast(IterContext*)context_;

        context.ret = context.dg(key, val);
        return context.ret == 0;
    }


    /***************************************************************************

        opApply over the complete contents of the storage engine. Note that this
        iteration is non-interruptible and should only be used in cases where
        you are certain that no other iterations will interfere with it.

    ***************************************************************************/

    public int opApply ( IterDg dg )
    {
        IterContext context;
        context.dg = dg;

        tcmdbforeach(this.db, &db_iter, cast(void*)&context);

        return context.ret;
    }


    /***********************************************************************

        Initialises a step-by-step iterator over the keys of all records in
        the database.

        Params:
            iterator = iterator to initialise

    ***********************************************************************/

    public typeof(this) getAll ( StorageEngineStepIterator iterator )
    {
        iterator.setStorage(this);

        return this;
    }


    /***************************************************************************

        Reset method, called when the storage engine is returned to the pool in
        IStorageChannels. Sends the Finish trigger to all registered listeners,
        which will cause the requests to end (as the channel being listened to
        is now gone).

    ***************************************************************************/

    public override void reset ( )
    {
        this.listeners.trigger(IListener.Code.Finish, 0);
    }


    /***************************************************************************

        Flushes sending data buffers of consumer connections.

    ***************************************************************************/

    public override void flush ( )
    {
        this.listeners.trigger(IListener.Code.Flush, 0);
    }


    /***************************************************************************

        Registers a listener with the channel. The dataReady() method of the
        given listener will be called when data is put to the channel.

        Params:
            listener = listener to notify when data is ready

     **************************************************************************/

    public void registerListener ( IListener listener )
    {
        this.listeners.register(listener);
    }


    /***************************************************************************

        Unregisters a listener from the channel.

        Params:
            listener = listener to stop notifying when data is ready

     **************************************************************************/

    public void unregisterListener ( IListener listener )
    {
        this.listeners.unregister(listener);
    }


    /***********************************************************************

        Performs any actions needed to safely close a channel. In the case
        of the memory database, nothing needs to be done.

        (Called from IStorageChannels when removing a channel or shutting
        down the node. In the former case, the channel is clear()ed then
        close()d. In the latter case, the channel is only close()d.)

        Returns:
            this instance

    ***********************************************************************/

    override public typeof(this) close ( )
    {
        return this;
    }


    /***********************************************************************

        Removes all records from database. We also move the dump file for this
        channel (if one has been written) to deleted/channel_name.tcm, in order
        to ensure that if the node is restarted the deleted channel will not be
        loaded again and restored!

        (Called from IStorageChannels when removing a channel.)

        Returns:
            this instance

    ***********************************************************************/

    override public typeof(this) clear ( )
    {
        tcmdbvanish(this.db);

        this.delete_channel(this.id);

        return this;
    }


    /***********************************************************************

        Returns:
            number of records stored

    ***********************************************************************/

    public ulong num_records ( )
    {
        ulong num;

        num = tcmdbrnum(this.db);

        return num;
    }


    /***********************************************************************

        Returns:
            number of bytes stored

    ***********************************************************************/

    public ulong num_bytes ( )
    {
        ulong size;

        size = tcmdbmsiz(this.db);

        return size;
    }


    /***********************************************************************

        Gets the first key in the database.

        Params:
            key = record key output

        Returns:
            true on success; false if there are no more keys to iterate

    ***********************************************************************/

    public bool getFirstKey ( out hash_t key )
    {
        tcmdbiterinit(this.db);
        return this.iterateNextKey(key);
    }


    /***********************************************************************

        Gets the key of the record following the specified key.

        Notes:
            * "following" means the next key in the TokyoCabinet storage, which
              is *not* necessarily the next key in numerical order.
            * If the last key has been removed, the iteration will be restarted
              at the closest key. As above, the exact meaning of "closest" is
              determined by TokyoCabinet.

        Params:
            last_key = key to iterate from
            key = record key output

        Returns:
            true on success; false if there are no more keys to iterate

    ***********************************************************************/

    public bool getNextKey ( hash_t last_key, out hash_t key )
    {
        tcmdbiterinit2(this.db, &last_key,
            castFrom!(size_t).to!(int)(last_key.sizeof));

        if ( !this.iterateNextKey(key) )
            return false;

        return this.iterateNextKey(key);
    }


    /**************************************************************************

        Iterates from the current iteration position, getting the key of next
        record in the database.

        Params:
            key = record key output

        Returns
            true on success; false if there are no more keys to iterate

    ***************************************************************************/

    private bool iterateNextKey ( out hash_t key )
    {
        int len;
        if ( auto key_ = cast(hash_t*)tcmdbiternext(this.db, &len) )
        {
            verify(len == hash_t.sizeof);
            auto str_len = hash_t.sizeof * 2;

            key = *key_;

            free(key_);

            return true;
        }
        else
            return false;
    }
}

version ( UnitTest )
{
    import ocean.core.Test;
    import Hash = swarm.util.Hash;
}

unittest
{
    auto storage = new StorageEngine("dummy", null, 1024, null);

    hash_t key1 = 0x0123456789abcdef;
    hash_t key2 = 0xfedcba9876543210;
    char[hash_t.sizeof * 2] key_str1, key_str2;
    Hash.toHexString(key1, key_str1);
    Hash.toHexString(key2, key_str2);
    cstring val1 = "value";
    cstring val2 = "another value";

    mstring buf, value;

    // Operations on a non-existent record.
    test(!storage.exists(key1));
    test!("==")(storage.getSize(key1), 0);
    test!("==")(storage.num_records(), 0);

    // Basic put / get.
    storage.put(key1, val1);
    storage.get(key1, buf, value);
    test!("==")(value, val1);

    // Basic get / put with string keys.
    storage.put(key_str1, val2);
    storage.get(key_str1, buf, value);
    test!("==")(value, val2);

    // Exists.
    test(storage.exists(key1));
    test(storage.exists(key_str1));

    // Num records.
    test!("==")(storage.num_records(), 1);

    // Remove.
    storage.remove(key1);
    test(!storage.exists(key1));
    test!("==")(storage.getSize(key1), 0);

    // Put then remove with string keys.
    storage.put(key_str1, val1);
    test(storage.exists(key1));
    storage.remove(key_str1);
    test(!storage.exists(key1));
    test!("==")(storage.getSize(key1), 0);

    // Put two records.
    storage.put(key1, val1);
    storage.put(key2, val2);
    test!("==")(storage.num_records(), 2);

    // Iteration.
    mstring[hash_t] iterated;
    foreach ( k, v; storage )
        iterated[k] = v.dup;
    test!("==")(iterated.length, 2);
    foreach ( k, v; iterated )
    {
        if ( k == key1 )
            test!("==")(v, val1);
        else
        {
            test!("==")(k, key2);
            test!("==")(v, val2);
        }
    }
}
