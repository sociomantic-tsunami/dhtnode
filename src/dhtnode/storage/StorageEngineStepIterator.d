/*******************************************************************************

    Storage engine step iterator

    copyright:
        Copyright (c) 2013-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.storage.StorageEngineStepIterator;

import ocean.transition;

/*******************************************************************************

    Memory storage engine iterator.

    You can reuse an instance to this class to iterate over different
    StorageEngine instances as long as you "reset" the iteration by calling
    setStorage(). (This is the reason that this is not a nested class of
    StorageEngine.

*******************************************************************************/

public class StorageEngineStepIterator
{
    import ocean.core.Verify;
    import dhtnode.storage.StorageEngine;
    import Hash = swarm.util.Hash;


    /***************************************************************************

        Reference to storage engine, set by setStorage() method.

    ***************************************************************************/

    private StorageEngine storage;


    /***************************************************************************

        Indicates if iteration has already started or finished. Determines what
        happens when next() is called.

    ***************************************************************************/

    private enum State
    {
        Init,
        Started,
        Finished
    }

    /// ditto
    private State state;


    /***************************************************************************

        Buffer to render record keys. The length of this buffer is never
        decreased, only increased (if necessary). This is an optimization, as
        keys are rendered extremely frequently and array length resetting is not
        free (especially in D2 builds, where assumeSafeAppend must be called).

    ***************************************************************************/

    private mstring key_buffer;


    /***************************************************************************

        Key of current record.

    ***************************************************************************/

    private hash_t current_key;


    /***************************************************************************

        Buffer to receive record values. The length of this buffer is never
        decreased, only increased (if necessary). This is an optimization, as
        values are fetched extremely frequently and array length resetting is
        not free (especially in D2 builds, where assumeSafeAppend must be
        called).

    ***************************************************************************/

    private mstring value_buffer;


    /***************************************************************************

        Storage initialiser.

        Params:
            storage = storage engine to iterate over

    ***************************************************************************/

    public void setStorage ( StorageEngine storage )
    {
        this.storage = storage;
        this.state = State.Init;
    }


    /***************************************************************************

        Starts the iteration at the specified key (instead of from the
        beginning).

        Params:
            key = hash representation of the key to set the iterator to

    ***************************************************************************/

    public void startFrom ( hash_t key )
    {
        this.current_key = key;
        this.state = State.Started;
    }


    /***************************************************************************

        Gets the key of the current record the iterator is pointing to.

        Returns:
            current key

    ***************************************************************************/

    public hash_t key ( )
    {
        return this.current_key;
    }


    /***************************************************************************

        Gets the key of the current record the iterator is pointing to, rendered
        as a hex string.

        Returns:
            current key rendered as a string

    ***************************************************************************/

    public mstring key_as_string ( )
    {
        if ( this.key_buffer.length < hash_t.sizeof * 2 )
            this.key_buffer.length = hash_t.sizeof * 2;

        Hash.toHexString(this.current_key, this.key_buffer);
        return this.key_buffer;
    }


    /***************************************************************************

        Gets the value of the current record the iterator is pointing
        to.

        Returns:
            current value

    ***************************************************************************/

    public mstring value ( )
    {
        verify(this.storage !is null,
            typeof(this).stringof ~ ".next: storage not set");

        mstring value_slice;
        this.storage.get(this.current_key, this.value_buffer, value_slice);
        return value_slice;
    }


    /***************************************************************************

        Gets the value of the current record the iterator is pointing
        to, passing the value to the provided delegate.

        Params:
            value_dg = delegate to pass the current value to

    ***************************************************************************/

    public void value ( scope void delegate ( cstring ) value_dg )
    {
        assert(this.storage, typeof(this).stringof ~ ".value: storage not set");

        this.storage.get(this.current_key, value_dg);
    }


    /***************************************************************************

        Advances the iterator to the next record or to the first record in the
        storage engine, if this.state is Init.

    ***************************************************************************/

    public void next ( )
    {
        verify(this.storage !is null,
            typeof(this).stringof ~ ".next: storage not set");

        bool more;
        with ( State ) switch ( this.state )
        {
            case Init:
                this.state = Started;
                more = this.storage.getFirstKey(this.current_key);
                break;
            case Started:
                more = this.storage.getNextKey(this.current_key,
                    this.current_key);
                break;
            case Finished:
                break;
            default:
                verify(false);
        }

        if ( !more )
            this.state = State.Finished;
    }


    /***************************************************************************

        Tells whether the current record pointed to by the iterator is the last
        in the iteration.

        This method may be overridden, but the default definition of the
        iteration end is that the current key is empty.

        Returns:
            true if the current record is the last in the iteration

    ***************************************************************************/

    public bool lastKey ( )
    {
        return this.state == State.Finished;
    }
}
