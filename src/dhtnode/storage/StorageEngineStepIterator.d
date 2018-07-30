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


    /***************************************************************************

        Reference to storage engine, set by setStorage() method.

    ***************************************************************************/

    private StorageEngine storage;


    /***************************************************************************

        Indicates if iteration has already started. If next() is called when
        this value is false, the iteration will be started.

    ***************************************************************************/

    private bool started;


    /***************************************************************************

        Buffer to receive record keys. The length of this buffer is never
        decreased, only increased (if necessary). This is an optimization, as
        keys are fetched extremely frequently and array length resetting is not
        free (especially in D2 builds, where assumeSafeAppend must be called).

    ***************************************************************************/

    private mstring key_buffer;


    /***************************************************************************

        Key of current record (slice of this.key_buffer).

    ***************************************************************************/

    private mstring current_key;


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
        this.started = false;
    }


    /***************************************************************************

        Gets the key of the current record the iterator is pointing to.

        Returns:
            current key

    ***************************************************************************/

    public mstring key ( )
    {
        return this.current_key;
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

        Advances the iterator to the next record or to the first record in the
        storage engine, if this.started is false.

    ***************************************************************************/

    public void next ( )
    {
        verify(this.storage !is null,
            typeof(this).stringof ~ ".next: storage not set");

        if (this.started)
            this.storage.getNextKey(this.current_key, this.key_buffer,
                this.current_key);
        else
        {
            this.started = true;
            this.storage.getFirstKey(this.key_buffer, this.current_key);
        }
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
        return this.key.length == 0;
    }
}
