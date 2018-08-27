/*******************************************************************************

    Class to manage the range of hashes handled by a DHT node, including:
        * The ability to modify the range and update the config file with the
          new range.
        * A facility for requests to register to be informed about changes to
          the hash range of this node or incoming information about the hash
          ranges of other nodes (via the Redistribute request).

    The hash range takes one of two forms:
        1. The standard form. Min hash <= max hash.
        2. Empty. Min hash and max hash both have magic values (see
           ocean.math.Range), allowing this state to be distinguished.

    The empty state is supported to allow new nodes to be started up with no
    current hash responsibility, awaiting an external command to tell them which
    range they should support. It could also be used to effectively delete a
    node by setting its hash range to empty.

    copyright:
        Copyright (c) 2014-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.node.DhtHashRange;

import dhtnode.config.HashRangeConfig;

import swarm.neo.AddrPort;
import swarm.util.Hash : HashRange;

import ocean.transition;
import ocean.core.Enforce;
import ocean.core.Verify;

/// ditto
public class DhtHashRange
{
    import swarm.Const : NodeItem;

    /***************************************************************************

        Min & max hash values.

    ***************************************************************************/

    private HashRange range_;


    /***************************************************************************

        Config file updater.

    ***************************************************************************/

    private HashRangeConfig config_file;


    /***************************************************************************

        Hash range update info listener set.

    ***************************************************************************/

    public HashRangeUpdates updates;

    /***************************************************************************

        Constructor. Sets the range as specified.

        Params:
            min = min hash
            max = max hash
            config_file = config file updater

        Throws:
            if the range specified by min & max is invalid

    ***************************************************************************/

    public this ( hash_t min, hash_t max, HashRangeConfig config_file )
    {
        verify(config_file !is null);
        this.config_file = config_file;

        enforce(HashRange.isValid(min, max), "Invalid hash range");
        this.range_ = HashRange(min, max);

        this.updates = new HashRangeUpdates;
    }


    /***************************************************************************

        Returns:
            hash range

    ***************************************************************************/

    public HashRange range ( )
    {
        return this.range_;
    }


    /***************************************************************************

        Returns:
            true if the hash range is empty

    ***************************************************************************/

    public bool is_empty ( )
    {
        return this.range.is_empty;
    }


    /***************************************************************************

        Sets the hash range, updates the config file(s), and informs any clients
        which are listening for hash range updates.

        Params:
            min = min hash
            max = max hash

        Throws:
            if the specified range is invalid

    ***************************************************************************/

    public void set ( hash_t min, hash_t max )
    {
        this.config_file.set(min, max);

        enforce(HashRange.isValid(min, max), "Invalid hash range");
        this.range_ = HashRange(min, max);

        this.updates.changedHashRange(min, max);
    }


    /***************************************************************************

        Called when information about another DHT node is available. Informs any
        clients which are listening for hash range updates.

        IMPORTANT NOTE:
        The support for forwarding information about DHT alterations during a
        redistribution is currently in somewhat of a hacked state. The following
        happens:
            1. The dhtredist tool triggers (legacy) Redistribute requests,
               informing nodes of the ip/(legacy) ports of other nodes to
               forward data to.
            2. The (legacy) Redistribute requests call this method, passing on
               the ip/(legacy) ports of the new nodes.
            3. This information must be passed on to the HashRangeUpdates
               instance, *but* that is a registry of *neo* clients which are
               interested in updates on new nodes / hash range changes. Sending
               them the ip/(legacy) port of the new node is no good.
            4. Here comes the work-around: as the legacy client / Redistribute
               request have no way of knowing about the neo ports of nodes, we
               have to *generate* the neo port. We simply add 100 to the legacy
               port. Thus, where redistributions are concerned, there is a
               convention of a DHT node's neo port being +100 from its legacy
               port.

        (When we have a neo implementation of the Redistribute request, this
        hack can be removed.)

        Params:
            node_item = ip/port of new node's legacy protocol
            min = min hash
            max = max hash

    ***************************************************************************/

    public void newNodeAdded ( NodeItem node_item, hash_t min, hash_t max )
    {
        AddrPort addr;
        addr.setAddress(node_item.Address);
        assert(node_item.Port <= ushort.max - 100);
        addr.port = cast(ushort)(node_item.Port + 100);
        this.updates.newNode(addr, min, max);
    }


    /***************************************************************************

        Sets the hash range to empty and updates the config file(s).

    ***************************************************************************/

    public void clear ( )
    {
        this.config_file.clear();
        this.updates.clear();
        this.range_ = this.range_.init;
    }
}



/*******************************************************************************

    Interface for an entity which wants to be informed of changes to the node's
    hash range or information about other nodes and their hash ranges.

*******************************************************************************/

public interface IHashRangeListener
{
    /***************************************************************************

        Called when either the hash range of this node has changed or
        information about another node is available.

    ***************************************************************************/

    void hashRangeUpdateAvailable ( );

    /***************************************************************************

        Required in order for a map of interface instances to be possible. (See
        HashRangeUpdates.) Can just return Object.toHash().

    ***************************************************************************/

    hash_t toHash ( );
}



/*******************************************************************************

    Helper class for DhtHashRange. Handles forwarding hash range updates to a
    set of IHashRangeListener instances. Has the following components:
        1. A set of IHashRangeListener instances and methods to add and remove
           instances from the set.
        2. Per IHashRangeListener instance, a queue of HashRangeUpdates (see
           dhtproto.node.neo.request.GetHashRange) and a method for an
           IHashRangeListener to get the next update in its queue.
        3. Methods to indicate that a hash range change has occurred.

*******************************************************************************/

private final class HashRangeUpdates
{
    import ocean.util.container.map.Map;
    import ocean.util.container.pool.FreeList;
    import dhtproto.node.neo.request.GetHashRange : HashRangeUpdate;

    /***************************************************************************

        Queue of hash range updates for a single IHashRangeListener.

    ***************************************************************************/

    private struct UpdateQueue
    {
        import swarm.neo.AddrPort;
        import dhtproto.node.neo.request.GetHashRange : HashRangeUpdate;
        import ocean.core.array.Mutation : removeShift;

        /// List of queued updates.
        private HashRangeUpdate[] updates;

        /***********************************************************************

            Adds an update to the list containing information about this node
            changing its hash range.

            Params:
                new_min = new minimum hash range of this node
                new_max = new maximum hash range of this node

        ***********************************************************************/

        public void changedHashRange ( hash_t new_min, hash_t new_max )
        {
            HashRangeUpdate update;
            update.self = true;
            update.min = new_min;
            update.max = new_max;

            this.updates ~= update;
        }

        /***********************************************************************

            Adds an update to the list containing information about another node
            and its hash range.

            Params:
                addr = address & port of other node
                min = minimum hash range of other node
                max = maximum hash range of other node

        ***********************************************************************/

        public void newNode ( AddrPort addr, hash_t min, hash_t max )
        {
            HashRangeUpdate update;
            update.addr = addr;
            update.min = min;
            update.max = max;

            this.updates ~= update;
        }

        /***********************************************************************

            Pops an update from the queue (if it is not empty) and returns it
            via the out parameter.

            Params:
                update = out value to receive popped update, if one exists

            Returns:
                true if an update was popped, false if the queue is empty

        ***********************************************************************/

        public bool getNextUpdate ( out HashRangeUpdate update )
        {
            if ( this.updates.length == 0 )
                return false;

            update = this.updates[0];
            removeShift(this.updates, 0);
            enableStomping(this.updates);

            return true;
        }

        /***********************************************************************

            Empties the queue.

        ***********************************************************************/

        public void clear ( )
        {
            this.updates.length = 0;
            enableStomping(this.updates);
        }
    }

    /// Free-list of recycled UpdateQueue instances.
    private FreeList!(UpdateQueue) update_queues_pool;

    /// Map of IHashRangeListener instances -> update queues.
    private StandardKeyHashingMap!(UpdateQueue*, IHashRangeListener)
        listener_update_queues;

    /***************************************************************************

        Constructor.

    ***************************************************************************/

    public this ( )
    {
        const estimated_num_clients = 1_000;
        this.update_queues_pool = new FreeList!(UpdateQueue);
        this.listener_update_queues =
            new StandardKeyHashingMap!(UpdateQueue*, IHashRangeListener)(
                estimated_num_clients);
    }

    /***************************************************************************

        Adds the specified IHashRangeListener to the set. It will be informed
        about all future hash range updates via its hashRangeUpdateAvailable()
        method.

        Params:
            listener = listener instance to add

    ***************************************************************************/

    public void register ( IHashRangeListener listener )
    in
    {
        assert((listener in this.listener_update_queues) is null,
            "Listener already registered");
    }
    out
    {
        assert((listener in this.listener_update_queues) !is null,
            "register() failed to add listener to set");
    }
    body
    {
        bool added;
        *(this.listener_update_queues.put(listener, added)) =
            this.update_queues_pool.get(new UpdateQueue);
        assert(added, "Listener already registered");
    }

    /***************************************************************************

        Removes the specified IHashRangeListener from the set. It will no longer
        be informed about future hash range updates.

        Params:
            listener = listener instance to remove

    ***************************************************************************/

    public void unregister ( IHashRangeListener listener )
    in
    {
        assert((listener in this.listener_update_queues) !is null,
            "Cannot unregister non-registered listener");
    }
    out
    {
        assert((listener in this.listener_update_queues) is null,
            "unregister() failed to remove listener from set");
    }
    body
    {
        auto removed = this.listener_update_queues.remove(listener,
            ( ref UpdateQueue* update_queue )
            {
                update_queue.clear();
                this.update_queues_pool.recycle(update_queue);
            }
        );
        assert(removed, "Cannot unregister non-registered listener");
    }

    /***************************************************************************

        Gets the next queued update for the specified listener, if one exists.

        Params:
            listener = listener instance to get the next queued update for
            update = out value to receive the next update, if one exists

        Returns:
            true if an update was popped, false if the queue is empty

    ***************************************************************************/

    public bool getNextUpdate ( IHashRangeListener listener,
        out HashRangeUpdate update )
    in
    {
        assert((listener in this.listener_update_queues) !is null,
            "Cannot get updates for non-registered listener");
    }
    body
    {
        auto update_queue = listener in this.listener_update_queues;
        return (*update_queue).getNextUpdate(update);
    }

    /***************************************************************************

        Informs all registered listeners that this node has changed its hash
        range.

        Params:
            new_min = new minimum hash range of this node
            new_max = new maximum hash range of this node

    ***************************************************************************/

    public void changedHashRange ( hash_t new_min, hash_t new_max )
    {
        foreach ( listener, update_queue; this.listener_update_queues )
        {
            update_queue.changedHashRange(new_min, new_max);
            listener.hashRangeUpdateAvailable();
        }
    }

    /***************************************************************************

        Informs all registered listeners about another node and its hash range.

        Params:
            addr = address & port of other node
            min = minimum hash range of other node
            max = maximum hash range of other node

    ***************************************************************************/

    public void newNode ( AddrPort addr, hash_t min, hash_t max )
    {
        foreach ( listener, update_queue; this.listener_update_queues )
        {
            update_queue.newNode(addr, min, max);
            listener.hashRangeUpdateAvailable();
        }
    }

    /***************************************************************************

        Unregisters all listeners and clears their update queues.

    ***************************************************************************/

    public void clear ( )
    {
        foreach ( listener, update_queue; this.listener_update_queues )
            this.update_queues_pool.recycle(update_queue);

        this.listener_update_queues.clear();
    }
}
