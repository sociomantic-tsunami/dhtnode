/*******************************************************************************

    Application class for DHT reidtribution tool.

    copyright:
        Copyright (c) 2014-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtredist.Redist;


/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import ocean.util.app.CliApp;

version ( UnitTest )
{
    import ocean.core.Test;
}


/*******************************************************************************

    dhtredist application class

*******************************************************************************/

public class DhtRedist : CliApp
{
    import Version;
    import dhtredist.RedistDhtClient;

    import ocean.core.Enforce;
    import ocean.io.Stdout;
    import ocean.io.select.EpollSelectDispatcher;
    import ocean.text.Arguments;

    import swarm.Const;
    import swarm.util.Hash;

    import dhtproto.client.legacy.DhtConst : NodeHashRange;
    import dhtproto.client.legacy.internal.request.params.RedistributeInfo;

    import ocean.core.Array : sort;


    /***************************************************************************

        Epoll instance

    ***************************************************************************/

    private EpollSelectDispatcher epoll;


    /***************************************************************************

        Mapping between a node (address/hash range) and a hash range

        TODO: this could probably be refactored as an AA

    ***************************************************************************/

    private struct RangeAssignment
    {
        NodeHashRange* node;
        HashRange* new_range;
    }


    /***************************************************************************

        Hash range -> node assignment strategy base class.

    ***************************************************************************/

    private abstract static class Strategy
    {
        /***********************************************************************

            Assesses the provided set of node hash ranges and assigns new ranges
            to the nodes, based on the strategy.

            The base class implementation only handles console output; the
            actual strategy logic is delegated to the derived class (via
            assignNewRanges_()).

            Params:
                nodes = original node/hash range assignment

            Returns:
                new node/hash range assignment

        ***********************************************************************/

        final public RangeAssignment[] assignNewRanges ( NodeHashRange[] nodes )
        {
            auto assigned_ranges = this.assignNewRanges_(nodes);

            Stdout.newline.green.formatln("Assigning new hash ranges to nodes:").
                default_colour;

            foreach ( i, assigned; assigned_ranges )
            {
                Stdout.formatln("\t{}:{} -> 0x{:X16}..0x{:X16}",
                    assigned.node.node.Address, assigned.node.node.Port,
                    assigned.new_range.min, assigned.new_range.max);
            }

            assert(assigned_ranges.length == nodes.length);
            return assigned_ranges;
        }

        abstract public RangeAssignment[] assignNewRanges_ ( NodeHashRange[] nodes );
    }

    /***************************************************************************

        Extend strategy. Sorts the nodes by hash range then redistributes the
        entire hash range between them.

    ***************************************************************************/

    private static class ExtendStrategy : Strategy
    {
        /***********************************************************************

            Assesses the provided set of node hash ranges and assigns new ranges
            to the nodes, based on the strategy.

            Params:
                nodes = original node/hash range assignment

            Returns:
                new node/hash range assignment

        ***********************************************************************/

        override public RangeAssignment[] assignNewRanges_ ( NodeHashRange[] nodes )
        {
            auto new_ranges = calculateHashRanges(nodes.length);
            assert(new_ranges.length == nodes.length);

            RangeAssignment[] assigned_ranges;

            foreach ( i, ref node; nodes )
            {
                assigned_ranges ~= RangeAssignment(&node, &new_ranges[i]);
            }

            return assigned_ranges;
        }
    }


    /***************************************************************************

        Subdivide strategy. Sorts the nodes by server then redistributes the
        hash range covered by each server between the nodes on that server.

    ***************************************************************************/

    private static class SubdivideStrategy : Strategy
    {
        import ocean.core.Array : pop;


        /***********************************************************************

            Assesses the provided set of node hash ranges and assigns new ranges
            to the nodes, based on the strategy.

            Params:
                nodes = original node/hash range assignment

            Returns:
                new node/hash range assignment

        ***********************************************************************/

        override public RangeAssignment[] assignNewRanges_ ( NodeHashRange[] nodes )
        {
            // Group nodes by address.
            NodeHashRange[][cstring] nodes_by_server;
            foreach ( node; nodes )
            {
                nodes_by_server[idup(node.node.Address)] ~= node;
            }

            // Divide the hash range of each server evenly between the nodes on
            // that server.
            RangeAssignment[] assigned_ranges;

            foreach ( ip, nodes_on_this_server; nodes_by_server )
            {
                // If the number of empty nodes on this server is evenly
                // divisible by the number of non-empty nodes, we can perform a
                // simple subdivision of each node into the corresponding empty
                // nodes.
                if ( this.canUseSimpleSubdivide(nodes_on_this_server) )
                {
                    assigned_ranges ~=
                        this.subdivideIndividualNodes(nodes_on_this_server);
                }
                // Otherwise, it's not possible to split each non-empty node
                // evenly; just perform a normal redistribution among the nodes
                // on this server.
                else
                {
                    Stdout.newline.yellow.formatln("Uneven subdivision, cannot "
                        ~ "sub-divide nodes individually, dividing by server")
                        .default_colour;
                    auto server_range = getServerHashRange(nodes_on_this_server,
                        ip);
                    auto new_ranges = calculateHashRanges(
                        nodes_on_this_server.length,
                        server_range.min, server_range.max);

                    foreach ( i, ref node; nodes_on_this_server )
                    {
                        assigned_ranges ~= RangeAssignment(&node, &new_ranges[i]);
                    }
                }
            }

            return assigned_ranges;
        }


        /***********************************************************************

            Checks whether the non-empty nodes in the specified list can be
            individually subdivided.

            Params:
                nodes = list of node hash ranges

            Returns:
                true if a simple subdivision of each node is possible

        ***********************************************************************/

        private static bool canUseSimpleSubdivide ( NodeHashRange[] nodes )
        {
            auto empty = numEmpty(nodes);
            auto non_empty = nodes.length - empty;

            return empty > 0 && non_empty > 0 && empty % non_empty == 0;
        }

        unittest
        {
            NodeHashRange empty, non_empty;
            non_empty.range = HashRange(0, 1);

            // Degenerate cases (no subdivision necessary).
            test!("==")(canUseSimpleSubdivide([]), false);
            test!("==")(canUseSimpleSubdivide([empty, empty]), false);
            test!("==")(canUseSimpleSubdivide([non_empty, non_empty]), false);

            // 1 -> 2
            test!("==")(canUseSimpleSubdivide([non_empty, empty]), true);

            // 2 -> 4
            test!("==")(canUseSimpleSubdivide(
                [non_empty, empty, non_empty, empty]), true);

            // 4 -> 6
            test!("==")(canUseSimpleSubdivide(
                [non_empty, non_empty, non_empty, non_empty, empty, empty]),
                false);

            // 2 -> 5
            test!("==")(canUseSimpleSubdivide(
                [non_empty, empty, non_empty, empty, non_empty]), false);

            // 3 -> 6
            test!("==")(canUseSimpleSubdivide(
                [non_empty, empty, non_empty, empty, non_empty, empty]), true);

            // 2 -> 7
            test!("==")(canUseSimpleSubdivide(
                [non_empty, non_empty,
                empty, empty, empty, empty, empty, empty, empty]), false);

            // 2 -> 8
            test!("==")(canUseSimpleSubdivide(
                [non_empty, non_empty,
                empty, empty, empty, empty, empty, empty, empty, empty]), true);
        }


        /***********************************************************************

            Assesses the provided set of node hash ranges for nodes on a single
            server (i.e. with the same IP) and assigns new ranges to the nodes,
            splitting each non-empty node into a number of empty nodes.

            Params:
                nodes_on_this_server = node/hash range assignments for server

            Returns:
                the newly assigned ranges for the nodes on this server

        ***********************************************************************/

        private RangeAssignment[] subdivideIndividualNodes (
            NodeHashRange[] nodes_on_this_server )
        in
        {
            assert(nodes_on_this_server.length > 0);
            auto ip = nodes_on_this_server[0].node.Address;
            foreach ( n; nodes_on_this_server )
                assert(n.node.Address == ip);
        }
        out ( assigned_ranges )
        {
            assert(assigned_ranges.length == nodes_on_this_server.length);
        }
        body
        {
            Stdout.newline.green.formatln("Sub-dividing nodes individually:")
                .default_colour;

            RangeAssignment[] assigned_ranges;
            auto len = nodes_on_this_server.length;
            auto num_non_empty = len - numEmpty(nodes_on_this_server);
            auto subdiv = len / num_non_empty;

            // Separate nodes into lists of empty and non-empty
            NodeHashRange*[] empties, nonempties;
            foreach ( ref node; nodes_on_this_server )
            {
                if ( node.range.is_empty )
                    empties ~= &node;
                else
                    nonempties ~= &node;
            }
            assert(empties.length + nonempties.length
                == nodes_on_this_server.length);

            // For each non-empty node, divide its hash range between some of
            // the empty nodes
            foreach ( nonempty; nonempties )
            {
                Stdout.formatln("\tSub-dividing existing node {}:{} "
                    ~ "= 0x{:X16}..0x{:X16}:",
                    nonempty.node.Address, nonempty.node.Port,
                    nonempty.range.min, nonempty.range.max);

                auto subdiv_ranges = calculateHashRanges(subdiv,
                    nonempty.range.min, nonempty.range.max);
                foreach ( i, ref subdiv_range; subdiv_ranges )
                {
                    NodeHashRange* node;

                    if ( i == 0 )
                    {
                        node = nonempty;

                        Stdout.formatln(
                            "\t\tRe-assigning to range 0x{:X16}..0x{:X16}",
                            subdiv_range.min, subdiv_range.max);
                    }
                    else
                    {
                        auto ok = empties.pop(node);
                        assert(ok);
                        Stdout.formatln(
                            "\t\tAssigning 0x{:X16}..0x{:X16} to {}:{}",
                            subdiv_range.min, subdiv_range.max,
                            node.node.Address, node.node.Port);
                    }

                    assigned_ranges ~= RangeAssignment(node, &subdiv_range);
                }
            }

            return assigned_ranges;
        }


        /***********************************************************************

            Counts the number of empty ranges in the provided list of node hash
            ranges.

            Params:
                nodes = list of node hash ranges

            Returns:
                number of empty ranges in `nodes`

        ***********************************************************************/

        private static size_t numEmpty ( NodeHashRange[] nodes )
        {
            size_t num_empty;

            foreach ( ref node; nodes )
            {
                if ( node.range.is_empty )
                    num_empty++;
            }

            return num_empty;
        }

        unittest
        {
            NodeHashRange empty, non_empty;
            non_empty.range = HashRange(0, 1);

            test!("==")(numEmpty([]), 0);
            test!("==")(numEmpty([non_empty]), 0);
            test!("==")(numEmpty([non_empty, non_empty, non_empty]), 0);
            test!("==")(numEmpty([empty]), 1);
            test!("==")(numEmpty([empty, empty, empty]), 3);
            test!("==")(numEmpty([empty, non_empty]), 1);
            test!("==")(numEmpty([non_empty, empty]), 1);
            test!("==")(numEmpty([empty, non_empty, empty]), 2);
        }


        /***********************************************************************

            Gets the total hash range covered by all nodes on a single server.

            Params:
                nodes = original node/hash range assignment, all nodes must be
                    on the same server
                ip = ip address of server (used for exception messages)

            Returns:
                total hash range covered by server

            Throws:
                * if a node has an invalid hash range
                * if no node has a defined hash range (all empty)
                * if a hash range gap or overlap exists

        ***********************************************************************/

        private static HashRange getServerHashRange (
            NodeHashRange[] nodes_on_server, cstring ip )
        in
        {
            assert(nodes_on_server.length);

            foreach ( nhr; nodes_on_server )
            {
                assert(nhr.node.Address == nodes_on_server[0].node.Address);
            }
        }
        body
        {
            HashRange server_range;

            foreach ( node; nodes_on_server )
            {
                enforce(node.range.is_valid, cast(istring) ("Node on " ~ ip ~
                    " has an invalid range"));

                // Empty node, doesn't affect server range.
                if ( node.range.is_empty ) continue;

                // First non-empty node, sets server range.
                if ( server_range.is_empty )
                {
                    server_range = node.range;
                    continue;
                }

                // Subsequent non-empty node, may widen server range.
                if ( node.range.min < server_range.min )
                {
                    server_range.min = node.range.min;
                }

                if ( node.range.max > server_range.max )
                {
                    server_range.max = node.range.max;
                }
            }

            // Cannot subdivide if no node has its hash range set
            enforce(!server_range.is_empty, cast(istring) ("No node on " ~ ip ~
                " has a defined range"));

            // Check that the sum of all the nodes' hash ranges == the server's
            // hash range, with no gaps or overlaps.
            HashRange[] hash_ranges;
            foreach ( node; nodes_on_server )
            {
                hash_ranges ~= node.range;
            }
            enforce(server_range.isTessellatedBy(sort(hash_ranges.dup)),
                cast(istring) ("Range gap or overlap on " ~ ip));

            return server_range;
        }

        unittest
        {
            // Generates a list of node hash ranges, all with the same ip
            NodeHashRange[] node_hash_ranges ( HashRange[] ranges )
            {
                NodeHashRange[] nhr;
                foreach ( range; ranges )
                {
                    nhr ~= NodeHashRange(NodeItem("some_ip".dup, 100), range);
                }
                return sort(nhr);
            }

            HashRange empty;

            // Single emtpy range disallowed
            testThrown!()(getServerHashRange(node_hash_ranges([empty]), ""));

            // Multiple emtpy ranges disallowed
            testThrown!()(getServerHashRange(
                node_hash_ranges([empty, empty, empty]), ""));

            // Single set range ok
            test!("==")(getServerHashRange(
                node_hash_ranges([HashRange(0, 1)]), ""),
                HashRange(0, 1));

            // Single set range, multiple empty ranges ok
            test!("==")(getServerHashRange(
                node_hash_ranges([HashRange(0, 1), empty, empty]), ""),
                HashRange(0, 1));

            // Multiple set ranges ok
            test!("==")(getServerHashRange(node_hash_ranges(
                [HashRange(0, 1), HashRange(2, 3), HashRange(4, 5)]), ""),
                HashRange(0, 5));
        }
    }


    /***************************************************************************

        Flag indicating whether to actually send to the dht nodes. The default
        is to perform a dry run but not actually make any changes to the dht.

    ***************************************************************************/

    private bool execute = false;


    /***************************************************************************

        Constructor.

    ***************************************************************************/

    public this ( )
    {
        const name = "dhtredist";
        const desc = "initiates a redistribution of dht data by changing the " ~
            "hash ranges of the nodes";
        OptionalSettings options;
        options.usage = "";
        options.help =
`Tool to initiate a redistribution of data within a dht. The standard use
case, when adding new nodes to a dht, is as follows:
    1. Set up the new nodes as required. You can initially set their hash
       ranges, in config.ini, to null (that is, min=0xffffffffffffffff,
       max=0x0000000000000000), indicating that they are empty. (This is
       optional; dhtredist does not handle empty nodes in any special way.)
    2. Generate a nodes xml file containing the address/port of all nodes,
       including the new ones to be added to the dht.
    3. Run dhtredist, passing it the created xml file.`;
        super(name, desc, version_info, options);

        this.epoll = new EpollSelectDispatcher;
    }


    /***************************************************************************

        Sets up the command line arguments which the application can handle.

        Params:
            app = application instance
            args = args set up instance

    ***************************************************************************/

    public override void setupArgs ( IApplication app, Arguments args )
    {
        args("src").aliased('S').params(1).conflicts("ranges").
            help("XML file describing dht -- should contain the address/port of " ~
            "all nodes to be affected, including those which are currently empty.");
        args("ranges").aliased('r').params(1).conflicts("src").
            help("Special mode to generate evenly distributed hash ranges for " ~
            "the specified number of nodes, sending the results to stdout. Does " ~
            "not contact the dht.");
        args("execute").aliased('x').help("Send calculated redistribution to the " ~
            "dht. This option must be set if you want to actually trigger a " ~
            "redistribution -- the default mode of the program is a dry run only.");
        args("strategy").aliased('s').params(1).restrict(["extend", "subdivide"]).
            defaults("extend").help("Sets the redistribution strategy. 'extend' " ~
            "mode should be used when adding new servers to the dht -- it " ~
            "completely redistributes the data among the total set of nodes. " ~
            "'subdivide' mode should be used when adding new nodes to existing " ~
            "dht servers -- the data is redistributed such that it is not sent " ~
            "between servers.");
    }


    /***************************************************************************

        Checks whether the command line arguments are valid.

        Params:
            app = application instance
            args = args set up instance

        Returns:
            error message or null if arguments are ok

    ***************************************************************************/

    protected override istring validateArgs ( IApplication app, Arguments args )
    {
        if ( !(args("src").assigned || args("ranges").assigned) )
        {
            return "Please specify an action (-S, -r)";
        }

        return null;
    }


    /***************************************************************************

        Application main run method. Parses arguments and runs the application.

        The logic has the following steps:
            1. Connect to dht nodes and query current hash ranges.
            2. Calculate new hash ranges, using the strategy specified by the
               user, and assign each node to one of the newly generated hash
               ranges. (We check at this stage whether any change in node hash
               range assignment has occurred, and bail out if not.)
            3. For each node, work out which nodes it will need to forward
               records to, based on the difference between its old and new hash
               range.
            4. Send the Redistribute requests, containing all of the information
               calculated above, to the dht nodes.

        Params:
            args = command line arguments as an Arguments instence

        Returns:
            status code to return to the OS

    ***************************************************************************/

    protected override int run ( Arguments args )
    {
        if ( args("execute").set )
        {
            this.execute = true;
            Stdout.yellow.formatln("EXECUTE MODE").default_colour;
        }

        if ( args("src").assigned )
        {
            // 1. Connect to dht nodes and query current hash ranges.
            const num_conns = 1;
            const queue_size = 256 * 1024;
            auto dht = new RedistDhtClient(this.epoll, num_conns, queue_size);
            auto xml = args.getString("src");
            dht.addNodes(xml);
            this.handshake(dht);

            auto nodes = this.getHashRanges(dht);

            // 2. Calculate new hash ranges, by strategy and assign each node to
            // one of the newly generated hash ranges.
            Strategy strategy;
            switch ( args.getString("strategy") )
            {
                case "extend":
                    strategy = new ExtendStrategy;
                    break;

                case "subdivide":
                    strategy = new SubdivideStrategy;
                    break;

                default:
                    assert(false);
            }
            assert(strategy);

            auto new_node_ranges = strategy.assignNewRanges(nodes);
            assert(new_node_ranges.length == nodes.length);

            // If no node ranges have changed, quit without doing anything.
            bool no_change = true;
            foreach ( new_node_range; new_node_ranges )
            {
                if ( new_node_range.node.range != *new_node_range.new_range )
                {
                    no_change = false;
                    break;
                }
            }

            if ( no_change )
            {
                Stdout.green.formatln("\nNo change in node hash ranges. " ~
                    "Not sending Redistribute requests.").default_colour;
                return 0;
            }

            // 3. For each node, work out which nodes it will need to forward
            // records to.
            auto redist_infos = this.getPerNodeRedistribution(new_node_ranges);

            // 4. Send the Redistribute requests to the dht nodes.
            this.redistribute(dht, new_node_ranges, redist_infos);

            return 0;
        }
        else if ( args("ranges").assigned )
        {
            auto num_nodes = args.getInt!(size_t)("ranges");
            auto node_ranges = this.calculateHashRanges(num_nodes);

            foreach ( node_range; node_ranges )
            {
                Stdout.formatln("0x{:X16} 0x{:X16}", node_range.min, node_range.max);
            }

            return 0;
        }

        assert(0);
    }


    /***************************************************************************

        Performs a dht handshake and throws upon failure.

        Params:
            dht = dht client to use

        Throws:
            upon failure of one of the handshake requests

    ***************************************************************************/

    private void handshake ( RedistDhtClient dht )
    {
        bool error;

        void handshake ( RedistDhtClient.RequestContext, bool ok )
        {
            // As the DHT's hash ranges are *not* expected to be consistent (see
            // explanation above), we ignore the ok value passed to this
            // callback. (This value is generated by a call to
            // DhtNodeRegistry.all_nodes_ok(), which checks for hash range
            // consistency.)
        }

        void notifier ( RedistDhtClient.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                Stderr.formatln("Client error during handshake: {}",
                    info.message(dht.msg_buf));
                error = true;
            }
        }

        dht.nodeHandshake(&handshake, &notifier);
        this.epoll.eventLoop();

        enforce(!error, "DHT handshake failed");
    }


    /***************************************************************************

        Contacts the dht nodes known to the provided client and queries their
        hash responsibility range.

        Params:
            dht = dht client to use

        Returns:
            sorted list of node addresses/hash ranges queried from the dht. (The
            list is sorted by hash range so that nodes will always be assigned
            the same hash range, over multiple runs of this tool.)

        Throws:
            upon dht error

    ***************************************************************************/

    private NodeHashRange[] getHashRanges ( RedistDhtClient dht )
    {
        NodeHashRange[] nodes;

        void hash_range_dg ( RedistDhtClient.RequestContext context,
            in cstring addr, ushort port, HashRange range )
        {
            nodes ~= NodeHashRange(NodeItem(addr.dup, port), range);
        }

        bool error;
        void notifier ( RedistDhtClient.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                Stderr.red.formatln("Error performing GetResonsibleRange: {}",
                    info.message(dht.msg_buf)).default_colour;
                error = true;
            }
        }

        Stdout.green.formatln("Getting current hash ranges of nodes:").default_colour;

        dht.assign(dht.getResponsibleRange(&hash_range_dg, &notifier));

        this.epoll.eventLoop();

        if ( error )
        {
            throw new Exception("GetResponsibleRange failed");
        }

        // Sort by hash range, for the purpose of a normalised display
        nodes.sort(
            ( NodeHashRange e1, NodeHashRange e2 )
            {
                return e1.range.min < e2.range.min;
            }
        );

        foreach ( node; nodes )
        {
            Stdout.formatln("\t{}:{} = 0x{:X16}..0x{:X16}", node.node.Address,
                node.node.Port, node.range.min, node.range.max);
        }

        return nodes;
    }


    /***************************************************************************

        Generates a list of hash ranges which together cover the complete hash
        range specified (defaults to the complete range of 64-bit integers),
        dividing it as evenly as possible between the specified number of nodes.

        Params:
            nodes = the number of nodes to divide the complete hash range
                between
            min = beginning of hash range to subdivide
            max = end of hash range to subdivide

        Returns:
            an array of HashRanges

    ***************************************************************************/

    static private HashRange[] calculateHashRanges ( size_t nodes,
        ulong min = hash_t.min, ulong max = hash_t.max )
    in
    {
        assert(nodes >= 1, "cannot generate hash ranges for 0 nodes");
        assert(min <= max);
    }
    out ( ranges )
    {
        assert(ranges.length == nodes, "wrong number of node hash ranges generated");
    }
    body
    {
        ulong start = min;
        ulong range = (max - min) / nodes;
        ulong carry;
        HashRange[] ranges;
        HashRange new_range;

        double fractional = cast(double)(max % nodes)
            / cast(double)nodes;

        double accumulated_fraction = 0.0;

        for ( ulong i = 0; i < nodes - 1; i++ )
        {
            carry = 0;
            accumulated_fraction += fractional;

            if ( accumulated_fraction >= 1.0 )
            {
                carry++;
                accumulated_fraction -= 1.0;
            }

            new_range = HashRange(start, start + range + carry);
            ranges ~= new_range;

            start += range + carry + 1;
        }

        new_range = HashRange(start, max);
        ranges ~= new_range;

        return ranges;
    }


    /***************************************************************************

        Check that calculateHashRanges() generates complete lists of hash
        ranges which fully cover the entire specified range, without gaps.

    ***************************************************************************/

    unittest
    {
        void testRange ( ulong min, ulong max )
        {
            const max_nodes = 1000;
            for ( uint num_nodes = 1; num_nodes <= max_nodes; num_nodes++ )
            {
                test(HashRange(min, max).isTessellatedBy(
                    calculateHashRanges(num_nodes, min, max)));
            }
        }

        testRange(hash_t.min, hash_t.max);
        testRange(0x7fffffffffffffff, hash_t.max);
        testRange(hash_t.min, 0x7fffffffffffffff);
        testRange(0x7fffffffffffffff, 0x9000000000000000);
    }


    /***************************************************************************

        For each node in the given list, work out which nodes have now taken
        over responsibility for part of its old hash range. This information is
        compiled in RedistributeInfo structs, ready to be sent to the nodes via
        Redistribute requests.

        Params:
            new_node_ranges = list of hash ranges assigned to nodes

        Returns:
            a set of RedistributeInfo structs, one per node in new_node_ranges,
            ready to be sent to the nodes via Redistribute requests

    ***************************************************************************/

    private RedistributeInfo[NodeItem] getPerNodeRedistribution (
        RangeAssignment[] new_node_ranges )
    out ( redist_infos )
    {
        assert(redist_infos.length == new_node_ranges.length);
    }
    body
    {
        RedistributeInfo[NodeItem] redist_infos;
        for ( size_t i; i < new_node_ranges.length; i++ )
        {
            // Check for other nodes whose new range overlaps node i's old range
            auto i_range = new_node_ranges[i].node.range;
            auto i_node = new_node_ranges[i].node.node;
            redist_infos[i_node] = RedistributeInfo(*new_node_ranges[i].new_range);

            for ( size_t j; j < new_node_ranges.length; j++ )
            {
                if ( i == j ) continue;

                auto j_range = *new_node_ranges[j].new_range;
                auto j_node = new_node_ranges[j].node.node;

                if ( i_range.overlaps(j_range) )
                {
                    redist_infos[i_node].redist_nodes ~=
                        NodeHashRange(j_node, j_range);
                }
            }
        }

        return redist_infos;
    }


    /***************************************************************************

        Send Redistribute requests to the dht nodes.

        Params:
            dht = dht client
            new_node_ranges = list of hash ranges assigned to nodes
            redist_infos = list of new hash ranges and nodes to which data must
                be transferred

    ***************************************************************************/

    private void redistribute ( RedistDhtClient dht,
        RangeAssignment[] new_node_ranges,
        RedistributeInfo[NodeItem] redist_infos )
    in
    {
        assert(dht !is null);
    }
    body
    {
        uint done; // incremented when a node finishes

        RedistributeInfo get_nodes ( RedistDhtClient.RequestContext context )
        {
            auto i = context.integer;
            auto i_node = new_node_ranges[i].node.node;
            return redist_infos[i_node];
        }

        void notifier ( RedistDhtClient.RequestNotification info )
        {
            if ( info.type == info.type.Finished )
            {
                if ( info.succeeded )
                {
                    done++;
                    auto pcnt_done = 100.0f *
                        cast(float)done / cast(float)new_node_ranges.length;
                    Stdout.green.formatln("{}:{} finished. {}% of nodes done.",
                        info.nodeitem.Address, info.nodeitem.Port, pcnt_done)
                        .default_colour.flush;
                }
                else
                {
                    Stderr.red.formatln("Error performing Redistribute: {}",
                        info.message(dht.msg_buf)).default_colour.flush;

                    // Reschedule Redistribute request
                    const retry_ms = 2_000;
                    dht.schedule(dht.redistribute(info.nodeitem.Address,
                        info.nodeitem.Port, &get_nodes, &notifier)
                        .context(info.context), retry_ms);
                }
            }
        }

        auto action = this.execute ? "Sending" : "Calculated";
        Stdout.newline.green.formatln("{} Redistribute requests to nodes:",
            action).default_colour;

        foreach ( i, new_node_range; new_node_ranges )
        {
            with ( new_node_range )
            {
                // Print overlap between new and old ranges for this node
                auto overlap = cast(double)node.range.overlapAmount(*new_range)
                    / cast(double)(node.range.max - node.range.min);
                Stdout.formatln("\t{}:{} = 0x{:X16}..0x{:X16} -> 0x{:X16}..0x{:X16} ({}% overlap)",
                    node.node.Address, node.node.Port, node.range.min,
                    node.range.max, new_range.min, new_range.max,
                    overlap * 100.0f);

                // Print info about nodes to which this node will forward data
                auto redist_info = node.node in redist_infos;
                if ( redist_info && redist_info.redist_nodes.length )
                {
                    for ( size_t j; j < redist_info.redist_nodes.length; j++ )
                    {
                        with ( redist_info.redist_nodes[j] )
                        {
                            Stdout.formatln("\t\tRedistribute data to: {}:{} = 0x{:X16}..0x{:X16}",
                                node.Address, node.Port, range.min, range.max);
                        }
                    }
                }
                else
                {
                    Stdout.formatln("\t\tNo data to be redistributed");
                }

                // Assign Redistribute request
                if ( this.execute )
                {
                    dht.assign(dht.redistribute(node.node.Address, node.node.Port,
                        &get_nodes, &notifier).context(i));
                }
            }
        }

        this.epoll.eventLoop();
    }
}

