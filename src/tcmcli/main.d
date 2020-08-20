/*******************************************************************************

    CLI tool to stream a TCM file to stdout or to stream from stdin to a TCM
    file.

    copyright:
        Copyright (c) 2015-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module tcmcli.main;


/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import ocean.util.app.CliApp;


/*******************************************************************************

    Main function. Parses command line arguments and either displays help or
    starts the tool.

    Params:
        cl_args = array with raw command line arguments

*******************************************************************************/

version (unittest) {} else
private int main ( istring[] cl_args )
{
    auto app = new TcmCli;
    return app.main(cl_args);
}


/*******************************************************************************

    TCM CLI application class

*******************************************************************************/

private class TcmCli : CliApp
{
    import Version;

    import dhtnode.storage.DumpFile;

    import swarm.util.RecordStream;

    import ocean.io.Stdout;
    import ocean.io.serialize.SimpleStreamSerializer : EofException;

    import ocean.io.Console : Cin, Cout;
    import ocean.io.FilePath;


    /***************************************************************************

        Constructor

    ***************************************************************************/

    public this ( )
    {
        static immutable name = "tcmcli";
        static immutable desc = "tcmcli: DHT dump file (TCM) command line tool";

        super(name, desc, version_info);
    }


    /***************************************************************************

        Function executed when command line arguments are set up (before
        parsing).

        Params:
            app = application instance
            args = command line arguments instance

    ***************************************************************************/

    override public void setupArgs ( IApplication app, Arguments args )
    {
        args("read").aliased('r').params(1).conflicts("write").
            help("Stream from named TCM file to stdout.");
        args("write").aliased('w').params(1).conflicts("read").
            help("Stream from stdin to named TCM file.");
    }


    /***************************************************************************

        Function executed after parsing the command line arguments.

        This function is only called if the arguments are valid so far.

        Params:
            app = application instance
            args = command line arguments instance

        Returns:
            string with an error message if validation failed, null otherwise

    ***************************************************************************/

    override public istring validateArgs ( IApplication app, Arguments args )
    {
        if ( args("read").assigned )
        {
            if ( !FilePath(args.getString("read")).exists )
            {
                return "Specified file path does not exist.";
            }
        }
        else if ( args("write").assigned )
        {
            if ( FilePath(args.getString("write")).exists )
            {
                return "Specified file path already exists.";
            }
        }
        else
        {
            return "Either 'read' (-r) or 'write' (-w) must be specified.";
        }

        return null;
    }


    /***************************************************************************

        Do the actual application work.

        This method is meant to be implemented by subclasses to do the actual
        application work.

        Params:
            args = Command line arguments as an Arguments instence

        Returns:
            status code to return to the OS

    ***************************************************************************/

    override protected int run ( Arguments args )
    {
        if ( args("read").assigned )
        {
            return this.fileToStdout(args.getString("read"));
        }
        else
        {
            assert(args("write").assigned);
            return this.stdinToFile(args.getString("write"));
        }

        assert(false);
    }


    /***************************************************************************

        Stream from the specified file to stdout.

        Params:
            file = file to stream from

        Returns:
            status code to return to the OS

    ***************************************************************************/

    private int fileToStdout ( cstring file )
    {
        auto tcm_reader = new ChannelLoader(new ubyte[64 * 1024], true);
        tcm_reader.open(file);
        scope ( exit ) tcm_reader.close();

        mstring buf;
        foreach ( k, v; tcm_reader )
        {
            Record r;
            r.key = cast(ubyte[])k;
            r.value = cast(ubyte[])v;
            r.serialize(Cout.stream, buf);
        }

        return 0;
    }


    /***************************************************************************

        Stream from stdin to the specified file.

        Params:
            file = file to stream to

        Returns:
            status code to return to the OS

    ***************************************************************************/

    private int stdinToFile ( cstring file )
    {
        bool error;

        auto tcm_writer = new ChannelDumper(new ubyte[64 * 1024], NewFileSuffix,
            true);
        tcm_writer.open(file);
        scope ( exit )
        {
            tcm_writer.close();

            if ( !error )
            {
                // Rename temp file to real output file name
                FilePath(tcm_writer.path).rename(file);
            }
        }

        mstring buf;
        while ( true )
        {
            Record r;
            try
            {
                r.deserialize(Cin.stream, buf);
            }
            catch ( EofException e )
            {
                // An I/O exception (EOF) is expected when reading a key
                return 0;
            }

            with ( Record.Type ) switch ( r.type )
            {
                case KeyValue:
                    tcm_writer.write(cast(cstring)r.key, cast(cstring)r.value);
                    break;

                default:
                    // TODO: we could, of course, enhance this tool to support
                    // converting other record types into valid DHT records.
                    Stderr.formatln("Unexpected record format. Only key:value "
                        ~ "records are supported currently.");
                    error = true;
                    return 1;
            }
        }

        assert(false);
    }
}
