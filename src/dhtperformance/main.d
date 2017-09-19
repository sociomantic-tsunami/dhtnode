/*******************************************************************************

    Dht performance tester

    copyright:
        Copyright (c) 2011-2017 sociomantic labs GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtperformance.main;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

private import Version;

private import ocean.io.Stdout;
private import ocean.io.Terminal;
private import ocean.io.console.Tables;

private import ocean.io.select.EpollSelectDispatcher;
private import ocean.io.select.timeout.TimerEventTimeoutManager;

private import ocean.text.Arguments;

private import ocean.text.convert.Formatter;

private import ocean.util.app.Application;
private import ocean.util.app.CliApp;
private import ocean.util.log.StaticTrace;

private import ocean.math.SlidingAverage;
private import ocean.math.Distribution;

private import swarm.Const;

private import dhtproto.client.DhtClient;

private import dhtproto.client.legacy.DhtConst;

private import ocean.core.Thread;

private import ocean.time.StopWatch;

private import Integer = ocean.text.convert.Integer_tango;

import core.sys.posix.time: timespec, nanosleep;

import ocean.core.Time;
import ocean.core.Array;


/*******************************************************************************

    Main

    Params:
        cl_args = command line arguments

*******************************************************************************/

int main ( istring[] cl_args )
{
    auto app = new DhtPerformance;
    return app.main(cl_args);
}



/*******************************************************************************

    Dht performance tester class

*******************************************************************************/

private class DhtPerformance : CliApp
{
    import ocean.core.Enforce;


    /***************************************************************************

        Epoll instance

    ***************************************************************************/

    protected EpollSelectDispatcher epoll;


    /***************************************************************************

        Time in seconds to wait between node handshake retries

    ***************************************************************************/

    private int retry_delay;


    /***************************************************************************

        Dht client.

    ***************************************************************************/

    private DhtClient dht;


    /***************************************************************************

        Stopwatches to time the individual requests and the request batches.

    ***************************************************************************/

    private StopWatch request_timer;

    private StopWatch batch_timer;


    /***************************************************************************

        Buffer for record being sent to the dht.

    ***************************************************************************/

    private mstring record;


    /***************************************************************************

        Number of requests to perform per test cycle.

    ***************************************************************************/

    private uint count;


    /***************************************************************************

        String of request to perform.

    ***************************************************************************/

    private cstring command;


    /***************************************************************************

        Channel to perform requests over.

    ***************************************************************************/

    private cstring channel;


    /***************************************************************************

        Flag to dis/enable display of the number of requests which exceeded the
        specified time (this.timeout).

    ***************************************************************************/

    private bool show_timeouts;


    /***************************************************************************

        Microsecond limit, used for counting the number of requests which
        exceeded a certain time.

    ***************************************************************************/

    private ulong timeout;


    /***************************************************************************

        DHT client's timeout facility (reconnects on timeout) time in ms.

        If 0, then the DHT client timeout facility is not used at all.

    ***************************************************************************/

    private uint client_timeout;


    /***************************************************************************

        The number of requests to perform in parallel.

    ***************************************************************************/

    private uint parallel;


    /***************************************************************************

        Counter used to track how many requests have been sent in parallel.

    ***************************************************************************/

    private uint parallel_count;


    /***************************************************************************

        Total number of iterations to perform (0 = infinite).

    ***************************************************************************/

    private uint iterations;


    /***************************************************************************

        Number of the current iteration.

    ***************************************************************************/

    private uint iteration_count;


    /***************************************************************************

        Delay to introduce between requests (in μs).

    ***************************************************************************/

    private ulong processing_time;


    /***************************************************************************

        Single key mode.

    ***************************************************************************/

    private bool single_key_mode;


    /***************************************************************************

        Key to query in single key mode.

    ***************************************************************************/

    private hash_t single_key;


    /***************************************************************************

        Buffer for dht client message formatting.

    ***************************************************************************/

    private mstring message_buffer;


    /***************************************************************************

        Approximate maximum request rate in queries per second.
        Useful for limiting the impact the tool has on the DHT.

    ***************************************************************************/

    private uint request_rate;


    /***************************************************************************

        Delay time in nanoseconds between each request

    ***************************************************************************/

    private ulong request_delay;


    /***************************************************************************

        Whether or not data should be displayed per node, or total

    ***************************************************************************/

    private bool per_node_mode;


    /***************************************************************************

        Number of tables to display per row in per-node-mode

    ***************************************************************************/

    private uint tables_per_row;


    /***************************************************************************

        A collection of info about the performance of an individual dht node
        (in per-node mode).
        This struct is also used to aggregate global performance info about
        the dht as a whole (i.e. all nodes).

    ***************************************************************************/

    private struct PerformanceInfo
    {

        /***************************************************************************

            In per-node-mode, the percentage slower-than-average a node must be
            to have its output highlighted in red.

        ***************************************************************************/

        private const alert_percentage = 0.2;


        /***********************************************************************

            Time distribution tracker

        ***********************************************************************/

        public Distribution!(ulong) requests;


        /***********************************************************************

            Average times measurer

        ***********************************************************************/

        public SlidingAverage!(ulong) avg_times;


        /***********************************************************************

            Count the number of client timeouts that happened

        ***********************************************************************/

        public ulong client_timeout_count;


        /***********************************************************************

            Number of request errors observed

        ***********************************************************************/

        public ulong error_count;


        /***********************************************************************

            Stores the total time taken so far for a whole iteration

        ***********************************************************************/

        public ulong total_time;


        /***********************************************************************

            Initialize the fields

        ***********************************************************************/

        public void init ( )
        in
        {
            assert(this.requests is null);
            assert(this.avg_times is null);
        }
        body
        {
            this.requests = new Distribution!(ulong);
            this.avg_times = new SlidingAverage!(ulong)(1_000);
        }


        /***********************************************************************

            Clear (reset to empty/0) each field

        ***********************************************************************/

        public void clear ( )
        {
            this.requests.clear();
            this.avg_times.clear();
            this.client_timeout_count = 0;
            this.error_count = 0;
            this.total_time = 0;
        }


        /***********************************************************************

            Checks if a node is slow by comparing the current median time of
            the node with the time given as a parameter.

            Params:
                cmp_time = The time to compare this node to.

            Returns:
                True if it is slower than (global_median * (1 + alert_percentage)).
                False otherwise.

        ***********************************************************************/

        private bool isNodeSlow ( double cmp_time )
        {
            auto node_median = this.requests.median;

            bool result = node_median > cmp_time * ( 1 + this.alert_percentage );

            return result;
        }
    }


    /***************************************************************************

        Information about global performance

    ***************************************************************************/

    private PerformanceInfo global_info;


    /***************************************************************************

        per-node-mode information map, from node to info

    ***************************************************************************/

    private PerformanceInfo[NodeItem] node_info;


    /***************************************************************************

        List of sorted nodes, for displaying tables in per-node-mode

    ***************************************************************************/

    private NodeItem[] sorted_nodes;


    /***************************************************************************

        Buffer of nodes to print, for displaying tables in per-node-mode

    ***************************************************************************/

    private NodeItem[] nodes_to_print;


    /***************************************************************************

        Buffer of table cells to print for per-node-mode

    ***************************************************************************/

    private Table.Cell[] cell_buf;


    /***************************************************************************

        Buffer of empty table cells to print for per-node-mode

    ***************************************************************************/

    private Table.Cell[] empty_cell_buf;


    /***************************************************************************

        String buffer

    ***************************************************************************/

    private mstring str_buf;


    /***************************************************************************

        Constructor.

    ***************************************************************************/

    public this ( )
    {
        const app_name = "dhtperformance";
        const app_desc = "Dht performance tester";

        this.epoll = new EpollSelectDispatcher(new TimerEventTimeoutManager);
        this.retry_delay = 2;
        super(app_name, app_desc, versionInfo);
    }


    /***************************************************************************

        Set up the arguments parser for the app.

        Params:
            app = application instance
            argument = arguments parser to initialize

    ***************************************************************************/

    override public void setupArgs ( IApplication app, Arguments args )
    {
        args("source").aliased('S').params(1).required.
            help("xml file listing dht nodes to connect to");
        args("channel").aliased('c').params(1).defaults("test").
            help("dht channel to operate on");
        args("number").aliased('n').params(1).defaults("1000").
            help("the number of requests to perform in each test "
            "cycle (default is 1000)");
        args("parallel").aliased('p').params(1).defaults("1").
            help("the number of parallel requests to perform (default is 1)");
        args("size").aliased('s').params(1).defaults("1024").
            help("size of record to put (in bytes, default is 1024)");
        args("single-key").aliased('k').params(1).
            help("always query the single specified key");
        args("command").aliased('m').params(1).defaults("exists").
            restrict(["exists", "get", "put"]).help("command to test (exists / "
            "get / put)");
        args("timeout").aliased('t').params(1).
            help("displays a count of the number of requests which"
            " took longer than the specified time (in μs)");
        args("client-timeout").aliased('T').params(1).defaults("0").
            help("time (in msec) to use for the DHT client's timeout"
            " facility when doing a request (reconnect on timeout)");
        args("iterations").aliased('i').params(1).defaults("0").
            help("number of test cycles to perform (default is 0, infinite)");
        args("processing-time").aliased('d').params(1).defaults("0").
            help("simulate some processing time (delay) when handling the "
            "requests (in μs, max 1 second. Bear in mind that the sleep() "
            "syscall which happens can take about 50 μs extra that are added "
            "to this value.)");
        args("request-rate").aliased('r').params(1).
            help("set the approximate maximum request rate "
            "in queries per second");
        args("per-node-mode").aliased('N').params(0, 1).defaults("4").
            help("display information on a per-node basis instead of total, "
            "optionally give the number of tables to display per row");
    }


    /***************************************************************************

        Checks whether the parsed command line args are valid.

        Params:
            app = the application
            args = command line arguments object to validate

        Returns:
            null if args are valid, otherwise an error message

    ***************************************************************************/

    override public istring validateArgs ( IApplication app, Arguments args )
    {
        // validate here
        if ( args.getInt!(ulong)("processing-time") >= 1_000_000 ) // 1 sec
        {
            return "--processing-time argument can't exceed 1.000.000 "
                " μs (1 second)";
        }

        if ( args.getInt!(uint)("request-rate") >= 1_000_000 )
        {
            return "--request-rate can't exceed 1.000.000 requests per second";
        }
        else if ( args.exists("request-rate") &&
                  args.getInt!(uint)("request-rate") < 1 )
        {
            return "--request-rate must be at least 1 request per second";
        }

        if ( args.getInt!(uint)("per-node-mode") == 0 )
        {
            return "--per-node-mode can't print less than 1 table per row";
        }

        return null;
    }


    /***************************************************************************

        Initialises this instance from the specified command line args.

        Params:
            app  = the application
            args = command line arguments object to read settings from

    ***************************************************************************/

    public override void processArgs ( IApplication app, Arguments args )
    {
        this.iterations = args.getInt!(uint)("iterations");

        this.count = args.getInt!(uint)("number");

        this.channel = args.getString("channel");

        this.command = args.getString("command");

        this.parallel = args.getInt!(uint)("parallel");

        this.show_timeouts = args.exists("timeout");

        this.timeout = args.getInt!(ulong)("timeout");

        this.client_timeout = args.getInt!(uint)("client-timeout");

        this.record.length = args.getInt!(size_t)("size");

        this.single_key_mode = args.exists("single-key");

        this.single_key = args.getInt!(hash_t)("single-key");

        this.processing_time = args.getInt!(ulong)("processing-time");

        this.request_rate = args.getInt!(uint)("request-rate");

        this.per_node_mode = args.exists("per-node-mode");

        this.tables_per_row = args.getInt!(uint)("per-node-mode");
    }


    /***************************************************************************

        Performs the performance test indicated by the command line arguments.

        Params:
            args = processed arguments
        Returns:
            0

    ***************************************************************************/

    override protected int run ( Arguments args )
    {
        this.dht = this.initClient( args.getString("source"),
                                       this.parallel, true );

        this.global_info.init;

        if ( this.request_rate )
        {
            // calculate delay between requests in nanoseconds
            this.request_delay = 1_000_000_000 / this.request_rate;
        }

        if ( this.per_node_mode )
        {
            foreach ( node ; this.dht.nodes )
            {
                NodeItem item;
                item.Address = node.address;
                item.Port = node.port;

                PerformanceInfo info;
                info.init;
                this.node_info[item] = info;
                this.sorted_nodes ~= item;
            }

            .sort(sorted_nodes);
        }

        // Startup message
        Stdout.formatln("Dht performance tester:");
        Stdout.formatln("    performing {} {} requests to channel '{}' each"
            " test cycle, with up to {} requets in parallel",
                count, command, channel, parallel);
        if ( command == "put" ) Stdout.formatln("    putting records of {}"
            " bytes", this.record.length);

        // Test cycle
        do
        {
            this.batch_timer.start;

            this.performRequests();

            this.display();

            if ( this.per_node_mode )
            {
                foreach ( node, ref info; this.node_info )
                {
                    info.clear;
                }
            }

            this.global_info.clear;

            this.iteration_count++;
        }
        while ( this.iterations == 0 || this.iteration_count < this.iterations);
        return 0;
    }


    /***************************************************************************

        Initialises a client, connecting to nodes specified in the xml config
        file.

        Params:
            xml = name of xml file defining nodes in cluster
            connections = the number of connections
            strict_handshake = if true, an exception is thrown if the node
                handshake fails
            request_queue_size = size of client request queues (in bytes)

        Returns:
            initialised client

    ***************************************************************************/

    private SchedulingDhtClient initClient ( cstring xml, uint connections,
        bool strict_handshake = true, size_t request_queue_size = 256 * 1024 )
    {
        Stdout.formatln("Initialising client connections from {}", xml);

        auto client = this.createClient(xml, connections, request_queue_size);

        bool error = this.handshake(xml, client, false);

        while (error)
        {
            Thread.sleep(ocean.core.Time.seconds(this.retry_delay));
            error = this.handshake(xml, client, false);
        }

        return client;
    }


    /***************************************************************************

        Helper method for initClient.
        Creates a new instance of a client to the cluster specified in the given
        xml file.

        Params:
            xml = name of xml file defining nodes in cluster
            connections = the number of connections
            request_queue_size = size of client request queues (in bytes)

        Returns:
            instance of client

    ***************************************************************************/

    private SchedulingDhtClient createClient ( cstring xml,
        uint connections, size_t request_queue_size = 256 * 1024 )
    {
        auto client =
            new SchedulingDhtClient(this.epoll, connections, request_queue_size);

        client.addNodes(xml);

        return client;
    }


    /***************************************************************************

        Helper method for initClient.
        Attempts to handshake with the given cluster.

        Params:
            xml = name of xml file defining nodes in cluster
            client = the client
            strict_handshake = if true, an exception is thrown if the node
                handshake fails

        Returns:
            true if an error occured during handshake, false if successful

        Throws:
            optionally asserts that no errors occurred during initialisation

    ***************************************************************************/

    private bool handshake ( cstring xml, SchedulingDhtClient client,
        bool strict_handshake = true )
    {
        bool error;

        void handshake ( SchedulingDhtClient.RequestContext, bool ok )
        {
            if ( !ok )
            {
                error = true;
            }
        }

        void notifier ( SchedulingDhtClient.RequestNotification info )
        {
            if ( info.type == info.type.Finished && !info.succeeded )
            {
                Stderr.formatln("Client error during handshake: {}",
                    info.message(client.msg_buf));
                error = true;
            }
        }

        client.nodeHandshake(&handshake, &notifier);

        this.epoll.eventLoop();

        if ( strict_handshake )
        {
            enforce(!error, cast(istring) (typeof(this).stringof ~
                ".initClient - error during client initialisation of " ~
                xml)
            );
        }

        return false;//error;
    }


    /***************************************************************************

        Performs and times a batch of requests (one iteration).

    ***************************************************************************/

    private void performRequests ( )
    {
        this.global_info.total_time = 0;
        this.parallel_count = 0;

        for ( uint i; i < this.count; i++ )
        {
            this.assignRequest(this.getKey(i));
            this.flush();
        }
        this.flush(true);
    }


    /***************************************************************************

        Gets the key to query for the current request. The logic of determining
        the key can be modified by certain command line arguments.

        Params:
            i = iteration counter

        Returns:
            record key to query

    ***************************************************************************/

    private hash_t getKey ( uint i )
    {
        if ( this.single_key_mode )
        {
            return this.single_key;
        }
        else
        {
            return i;
        }
    }


    /***************************************************************************

        Assigns a single request to the dht.

        Params:
            key = record key

    ***************************************************************************/

    private void assignRequest ( hash_t key )
    {
        switch ( this.command )
        {
            case "exists":
                this.dht.assign(this.dht.exists(
                        this.channel, key, &this.existsCallback, &this.notifier).
                    timeout(this.client_timeout));
            break;
            case "put":
                this.dht.assign(this.dht.put(
                        this.channel, key, &this.putCallback, &this.notifier).
                    timeout(this.client_timeout));
            break;

            case "get":
                this.dht.assign(this.dht.get(
                        this.channel, key, &this.getCallback, &this.notifier).
                    timeout(this.client_timeout));
            break;

            default:
                assert(false);
        }
    }


    /***************************************************************************

        Checks whether the number of assigned dht requests is equal to the
        number of parallel requests specified on the command line, and calls the
        epoll event loop when this amount is reached, causing the requests to be
        performed.

        When the event loop exits, the console time display is updated.

        Params:
            force = if true, always flush, even if the number of parallel
                requests has not been reached (used at the end of a cycle to
                ensure that all requests have been processed)

    ***************************************************************************/

    private void flush ( bool force = false )
    {
        // How often the progress should be printed, in microseconds
        const ulong progress_delay_microsec = 200_000;

        if ( force || ++this.parallel_count == this.parallel )
        {
            this.parallel_count = 0;
            this.request_timer.start;
            this.epoll.eventLoop;

            if ( this.batch_timer.microsec > progress_delay_microsec )
            {
                if ( this.per_node_mode )
                {
                    this.printProgressPerNode(force);
                }
                else
                {
                    this.printTotalProgress();
                }

                this.batch_timer.start;
            }

            this.delay();
        }
    }


    /***************************************************************************

        Print test cycle progress for each node

        Params:
            last_print = if true, then this is the last progress print
                of this cycle, and the cursor will not be moved back
                if false, cursor is moved back so progress can be updated later

    ***************************************************************************/

    private void printProgressPerNode ( bool last_print = false )
    {
        // store no. rows printed so we can move back cursor later
        int rows_printed = 0;

        foreach ( node, info ; this.node_info )
        {
            this.printProgress(info, &node);

            rows_printed++;
        }

        if ( !last_print )
        {
            // progress per node printed, move cursor back
            while ( rows_printed > 0 )
            {
                Stdout.up;
                rows_printed--;
            }
            Stdout.cr;
        }
    }


    /***************************************************************************

        Print total progress of test cycle, against all nodes

    ***************************************************************************/

    private void printTotalProgress ( )
    {
        this.printProgress(this.global_info);
        Stdout.up;
        Stdout.cr;
    }


    /***************************************************************************

        Prints the progress of the given performance info

        Params:
            info = A record of performance info for a node, or globally.
            node = If not null, print the ip and port of this specific node.

    ***************************************************************************/

    private void printProgress ( PerformanceInfo info, NodeItem* node = null )
    {
        auto total_s = cast(float)info.total_time / 1_000_000.0;

        if ( node !is null )
        {
            Stdout.format("Node at {}:{}: ", node.Address, node.Port);
        }

        if ( this.client_timeout )
        {
            Stdout.formatln("avg: {}μs, count: {}, timeout: {}, total: {}s",
                info.avg_times.average, info.requests.count,
                info.client_timeout_count, total_s);
        }
        else
        {
            Stdout.formatln("avg: {}μs, count: {}, total: {}s",
                info.avg_times.average, info.requests.count, total_s);
        }
    }


    /***************************************************************************

        Called at the end of an iteration. Displays time distribution info about
        the requests which were performed.

    ***************************************************************************/

    private void display ( )
    {
        if ( this.per_node_mode )
        {
            this.printProgressPerNode(true);
        }
        else
        {
            this.printTotalProgress();
        }

        const percentages = [0.5, 0.66, 0.75, 0.8, 0.9, 0.95, 0.98, 0.99, 0.995, 0.999, 1];

        Stdout.formatln("");

        if ( this.iterations == 0 )
        {
            Stdout.formatln("Iteration {} of infinite. Time distribution of {} {} requests:",
                    this.iteration_count + 1, this.count, this.command);
        }
        else
        {
            Stdout.formatln("Iteration {} of {}. Time distribution of {} {} requests:",
                    this.iteration_count + 1, this.iterations, this.count, this.command);
        }

        if ( this.per_node_mode )
        {
            // only print the specified number of nodes per table row
            this.nodes_to_print.length = 0;
            for ( int i = 0; i < this.sorted_nodes.length; i++ )
            {
                this.nodes_to_print ~= this.sorted_nodes[i];

                if ( (i + 1) % this.tables_per_row == 0 ||
                     i == this.sorted_nodes.length - 1)
                {
                    this.displayTables(percentages, this.nodes_to_print);
                    this.nodes_to_print.length = 0;
                }
            }

            Stdout.newline;
            Stdout.formatln("Total times:");
            Stdout.newline;
        }

        foreach ( i, percentage; percentages )
        {
            auto time = this.global_info.requests.percentValue(percentage);

            Stdout.formatln("{,5:1}% <= {}μs{}",
                    percentage * 100, time,
                    (i == percentages.length - 1) ? " (longest request)" : "");
        }

        Stdout.formatln("\n{} requests ({,3:1}%) failed",
                this.global_info.error_count,
                (cast(float)this.global_info.error_count / cast(float)this.global_info.requests.length) * 100.0);

        if ( this.show_timeouts )
        {
            auto timed_out = this.global_info.requests.greaterThanCount(this.timeout);

            Stdout.formatln("{} requests ({,3:1}%) took longer than {}μs",
                    timed_out,
                    (cast(float)timed_out / cast(float)this.global_info.requests.length) * 100.0,
                    this.timeout);
        }

        if ( this.client_timeout )
        {
            auto to_count = this.global_info.client_timeout_count;

            Stdout.formatln("{} requests ({,3:1}%) timed out",
                    to_count,
                    (cast(float)to_count / cast(float)this.global_info.requests.length) * 100.0);
        }

        Stdout.formatln("");
    }

    /***************************************************************************

        Called at the end of an iteration. Displays time distribution info about
        the requests per node in tables if per-node-mode is running

        Params:
            percentages = The percentages of the distribution to display
            nodes = The list of nodes to print info about in the table

    ***************************************************************************/

    private void displayTables ( in double[] percentages, NodeItem[] nodes )
    {
        // re-calculate global median once per iteration
        auto global_median = this.global_info.requests.median;

        auto table = new Table(nodes.length);

        // header row
        table.firstRow.setDivider;
        this.cell_buf.length = 0;

        foreach ( node; nodes )
        {
            this.str_buf.length = 0;
            sformat(this.str_buf, "{}:{}", node.Address, node.Port);
            this.cell_buf ~= Table.Cell.String(this.str_buf);
        }

        table.nextRow.set(this.cell_buf);
        table.nextRow.setDivider;

        // statistics rows
        foreach ( i, percentage; percentages )
        {
            this.cell_buf.length = 0;

            foreach ( node; nodes )
            {
                PerformanceInfo info = this.node_info[node];

                this.str_buf.length = 0;

                auto time = info.requests.percentValue(percentage);

                sformat(this.str_buf, "{,5:1}% <= {}μs",
                    percentage * 100, time);

                // only red the last row if this node is slow
                bool highlight =
                    i == percentages.length - 1 && info.isNodeSlow(global_median);

                this.addCell(this.str_buf, highlight);
            }

            table.nextRow.set(this.cell_buf);
        }

        // empty row
        this.empty_cell_buf.length = 0;

        foreach ( node; nodes )
        {
            this.empty_cell_buf ~= Table.Cell.Empty;
        }

        table.nextRow.set(this.empty_cell_buf);

        // error row
        this.cell_buf.length = 0;

        foreach ( node; nodes )
        {
            PerformanceInfo info = this.node_info[node];

            this.str_buf.length = 0;

            sformat(this.str_buf, "{} ({,3:1}%) failed",
                info.error_count,
                (cast(float)info.error_count / cast(float)info.requests.length) * 100.0);

            this.addCell(this.str_buf, info.error_count > 0);
        }

        table.nextRow.set(this.cell_buf);

        // timeout row
        if ( this.show_timeouts )
        {
            this.cell_buf.length = 0;

            foreach ( node; nodes )
            {
                PerformanceInfo info = this.node_info[node];

                this.str_buf.length = 0;

                auto timed_out = info.requests.greaterThanCount(this.timeout);

                sformat(this.str_buf, "{} ({,3:1}%) > {}μs",
                        timed_out,
                        (cast(float)timed_out / cast(float)info.requests.length) * 100.0,
                        this.timeout);

                this.addCell(this.str_buf, timed_out > 0);
            }

            table.nextRow.set(this.empty_cell_buf);
            table.nextRow.set(this.cell_buf);
        }

        // client timeout row
        if ( this.client_timeout )
        {
            this.cell_buf.length = 0;

            foreach ( node; nodes )
            {
                PerformanceInfo info = this.node_info[node];

                this.str_buf.length = 0;

                auto to_count = info.client_timeout_count;

                sformat(this.str_buf, "{} ({,3:1}%) timed out",
                    to_count,
                    (cast(float)to_count / cast(float)info.requests.length) * 100.0);

                this.addCell(this.str_buf, to_count > 0);
            }

            table.nextRow.set(this.empty_cell_buf);
            table.nextRow.set(this.cell_buf);
        }

        // final divider
        table.nextRow.setDivider;

        Stdout.newline;
        table.display;
    }


    /***************************************************************************

        Adds a cell to the cell buffer with the given string contents.
        Colors the cell red if highlight is true.

        Params:
            str = the string contents of the cell
            highlight = if true, colors the cell red

    ***************************************************************************/

    private void addCell ( cstring str, bool highlight )
    {
        auto cell = Table.Cell.String(str);

        if ( highlight )
        {
            cell.setForegroundColour(Terminal.Colour.Red);
        }

        this.cell_buf ~= cell;
    }


    /***************************************************************************

        Dht put callback.

        Params:
            context = request context (unused)

        Returns:
            record to put

    ***************************************************************************/

    private cstring putCallback ( DhtClient.RequestContext context )
    {
        return this.record;
    }


    /***************************************************************************

        Dht getcallback.

        Params:
            context = request context (unused)
            data = record got

    ***************************************************************************/

    private void getCallback ( DhtClient.RequestContext context, in cstring data )
    {

    }


    /***************************************************************************

        Dht exists callback.

        Params:
            context = request context (unused)
            data = record got

    ***************************************************************************/

    private void existsCallback ( DhtClient.RequestContext context, bool data )
    {

    }


    /***************************************************************************

        Delay the next request if request_rate has a value,
        or if processing_time has a value.

        Calculate the amount of time we should delay for in nanoseconds.
        The request delay time, minus the time it took for the requests,
        plus processing_time if the user has specified this.
        If this is 0 or less, do not delay.

    ***************************************************************************/

    private void delay ( )
    {
        ulong sleep_time = this.processing_time * 1000;

        if ( this.request_rate > 0 )
        {
            ulong requests_time = cast(ulong)this.global_info.avg_times.average *
                this.parallel * 1_000;
            sleep_time += (this.request_delay - requests_time);
        }

        // Add delay time to each node info when running in per-node-mode
        if ( this.per_node_mode )
        {
            foreach ( node, ref info ; this.node_info )
            {
                info.total_time += (sleep_time / 1000);
            }
        }

        this.global_info.total_time += (sleep_time / 1000);

        this.sleepNanoseconds(sleep_time <= 0 ? 0 : sleep_time);
    }


    /***************************************************************************

        Sleep for the given number of nanoseconds

        Params:
            nanoseconds = number of nanoseconds to sleep for

    ***************************************************************************/

    private void sleepNanoseconds ( ulong nanoseconds )
    {
        if ( nanoseconds == 0 )
        {
            return;
        }

        // if nanoseconds exceeds 1,000,000,000
        // the billions go in the "seconds part"
        // and the remainder goes in the "nanoseconds part"
        ulong seconds = 0;
        if ( nanoseconds >= 1_000_000_000 )
        {
            seconds = nanoseconds / 1_000_000_000;
            nanoseconds = nanoseconds - (seconds * 1_000_000_000);
        }

        timespec t;
        t.tv_sec = seconds;
        t.tv_nsec = nanoseconds;
        nanosleep(&t, null); // ignore reminder, we don't care that much
    }


    /***************************************************************************

        Dht notification callback. Updates the timers with the time taken to
        complete this request.

        Params:
            info = request notification info

    ***************************************************************************/

    private void notifier ( DhtClient.RequestNotification info )
    {
        if ( info.type == info.type.Finished )
        {
            if ( info.succeeded || ( this.client_timeout && info.timed_out ))
            {
                auto Us = this.request_timer.microsec;

                if ( this.per_node_mode )
                {
                    this.node_info[info.nodeitem].requests ~= Us;
                    this.node_info[info.nodeitem].avg_times.push(Us);
                    this.node_info[info.nodeitem].total_time += Us;
                }

                this.global_info.requests ~= Us;
                this.global_info.avg_times.push(Us);
                this.global_info.total_time += Us;

                if ( info.timed_out )
                {
                    if ( this.per_node_mode )
                    {
                        this.node_info[info.nodeitem].client_timeout_count++;
                    }

                    this.global_info.client_timeout_count++;
                }
            }
            else
            {
                if ( this.per_node_mode )
                {
                    this.node_info[info.nodeitem].error_count++;
                }

                this.global_info.error_count++;
                Stderr.formatln("Error in dht request: {}",
                    info.message(this.message_buffer));
            }
        }
    }
}
