/*******************************************************************************

    Classes for reading and writing dht node channel dump files.

    copyright:
        Copyright (c) 2014-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.storage.DumpFile;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dhtnode.storage.DirectIO;

import ocean.io.FilePath;

import ocean.io.serialize.SimpleStreamSerializer;

import ocean.io.model.IConduit : InputStream;

import ocean.io.device.File;

import ocean.util.log.Logger;

import Integer = ocean.text.convert.Integer_tango;



/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ( )
{
    log = Log.lookup("dhtnode.storage.DumpFile");
}



/*******************************************************************************

    Dump file format version number.

*******************************************************************************/

public static immutable ulong FileFormatVersion = 0;


/*******************************************************************************

    File suffix constants

*******************************************************************************/

public static immutable DumpFileSuffix = ".tcm";

public static immutable NewFileSuffix = ".dumping";


/*******************************************************************************

    Direct I/O files buffer size.

    See BufferedDirectWriteFile for details on why we use 32MiB.

*******************************************************************************/

public static immutable IOBufferSize = 32 * 1024 * 1024;


/*******************************************************************************

    Formats the file name for a channel into a provided FilePath. The name
    is built using the specified root directory, the name of the channel and
    the standard file type suffix.

    Params:
        root = FilePath object denoting the root dump files directory
        path = FilePath object to set with the new file path
        channel = name of the channel to build the file path for

    Returns:
        The "path" object passed as parameter and properly reset.

*******************************************************************************/

public FilePath buildFilePath ( FilePath root, FilePath path, cstring channel )
{
    path.set(root);
    path.append(channel);
    path.cat(DumpFileSuffix);
    return path;
}


/*******************************************************************************

    Atomically replace the existing dump with the new one.

    Params:
        dumped_path = path of the file to which the dump was written (dump.new)
        channel = name of dump file (without suffix)
        root = path of dump files' directory
        path = FilePath object used for file swapping
        swap_path = FilePath object used for file swapping

*******************************************************************************/

public void rotateDumpFile ( cstring dumped_path, cstring channel, FilePath root,
    FilePath path, FilePath swap_path )
{
    path.set(dumped_path); // dump.new
    buildFilePath(root, swap_path, channel); // dump
    path.rename(swap_path);
}


/*******************************************************************************

    Dump file writer.

*******************************************************************************/

public class ChannelDumper
{
    /***************************************************************************

        Output buffered direct I/O file, used to dump the channels.

    ***************************************************************************/

    private BufferedDirectWriteTempFile output;


    /***************************************************************************

        Constructor.

        Params:
            buffer = buffer used by internal direct I/O writer
            suffix = suffix to use for temporary files used while dumping
            disable_direct_io = determines if regular buffered I/O (true) or
                                direct I/O is used (false). Regular I/O is only
                                useful for testing, because direct I/O imposes
                                some restrictions over the type of filesystem
                                that can be used.

    ***************************************************************************/

    public this ( ubyte[] buffer, cstring suffix, bool disable_direct_io )
    {
        this.output = new BufferedDirectWriteTempFile(null, buffer, suffix,
                disable_direct_io);
    }


    /***************************************************************************

        Opens the dump file for writing and writes the file format version
        number at the beginning.

        Params:
            path = path to open

    ***************************************************************************/

    public void open ( cstring path )
    {
        this.output.open(path);

        SimpleStreamSerializer.write(this.output, FileFormatVersion);
    }


    /***************************************************************************

        Returns:
            the path of the open file

    ***************************************************************************/

    public cstring path ( )
    {
        return this.output.path();
    }

    /***************************************************************************

        Writes a record key/value to the file.

        Params:
            key = record key
            value = record value

    ***************************************************************************/

    public void write ( cstring key, cstring value )
    {
        SimpleStreamSerializer.write(this.output, key);
        SimpleStreamSerializer.write(this.output, value);
    }


    /***************************************************************************

        Closes the dump file, writing the requisite end-of-file marker (an empty
        string) at the end.

    ***************************************************************************/

    public void close ( )
    {
        static immutable istring end_of_file = "";
        SimpleStreamSerializer.write(this.output, end_of_file);

        this.output.close();
    }
}



/*******************************************************************************

    Dump file reader base class.

    (Works with an abstract InputStream, rather than a file, in order to allow
    unittests to use this class with other forms of stream, avoiding disk
    access.)

*******************************************************************************/

abstract public class ChannelLoaderBase
{
    import ocean.core.Enforce;

    /***************************************************************************

        Base class encapsulating the file-format-neutral process of reading
        records from the file.

    ***************************************************************************/

    private abstract class FormatReaderBase
    {
        /***********************************************************************

            foreach iterator over key/value pairs in the file. Reads until the
            caller aborts the iteration or getRecord() returns false.

        ***********************************************************************/

        public int opApply ( scope int delegate ( ref mstring key, ref mstring value ) dg )
        {
            int res;

            do
            {
                if ( !this.getRecord(this.outer.load_key, this.outer.load_value) )
                    break;

                res = dg(this.outer.load_key, this.outer.load_value);
                if ( res ) break;
            }
            while ( true );

            return res;
        }


        /***********************************************************************

            Reads the next record, if one exists.

            Params:
                key = buffer to receive key of next record, if one exists
                value = buffer to receive value of next record, if one exists

            Returns:
                true if another record exists and has been read, false if there
                are no more

        ***********************************************************************/

        abstract protected bool getRecord ( ref mstring key, ref mstring value );
    }


    /***************************************************************************

        File format version 0 reader.

    ***************************************************************************/

    private class FormatReader_v0 : FormatReaderBase
    {
        /***********************************************************************

            Reads the next record, if one exists. The end of the file is marked
            by a key of length 0.

            Params:
                key = buffer to receive key of next record, if one exists
                value = buffer to receive value of next record, if one exists

            Returns:
                true if another record exists and has been read, false if there
                are no more

        ***********************************************************************/

        override public bool getRecord ( ref mstring key, ref mstring value )
        {
            SimpleStreamSerializer.read(this.outer.input, key);
            if ( key.length == 0 ) return false;

            SimpleStreamSerializer.read(this.outer.input, value);

            return true;
        }
    }


    /***************************************************************************

        Minimum and maximum supported file format version numbers

    ***************************************************************************/

    private static immutable ulong min_supported_version = 0;

    private static immutable ulong max_supported_version = 0;

    static assert(min_supported_version <= max_supported_version);


    /***************************************************************************

        Input stream, used to load the channel dumps.

    ***************************************************************************/

    protected InputStream input;


    /***************************************************************************

        Key and value read buffers.

    ***************************************************************************/

    private mstring load_key, load_value;


    /***************************************************************************

        File format version read from beginning of file. Stored so that it can
        be quired by the user (see file_format_version(), below).

    ***************************************************************************/

    private ulong file_format_version_;


    /***************************************************************************

        Constructor.

        Params:
            input = input stream to load channel data from

    ***************************************************************************/

    public this ( InputStream input )
    {
        this.input = input;
    }


    /***************************************************************************

        Opens the dump file for reading and reads the file format version number
        at the beginning.

        NOTE: in the old file format, the first 8 bytes actually store the
        number of records contained in the file.

        Params:
            path = path to open

    ***************************************************************************/

    public void open ( )
    {
        SimpleStreamSerializer.read(this.input, this.file_format_version_);
    }


    /***************************************************************************

        Returns:
            the file format version number read when the file was opened

    ***************************************************************************/

    public ulong file_format_version ( )
    {
        return this.file_format_version_;
    }


    /***************************************************************************

        Returns:
            whether the file format version is supported

    ***************************************************************************/

    public bool supported_file_format_version ( )
    {
        return this.file_format_version_ >= this.min_supported_version &&
            this.file_format_version_ <= this.max_supported_version;
    }


    /***************************************************************************

        Returns:
            the number of bytes contained in the file, excluding the 8 byte file
            format version number

    ***************************************************************************/

    final public ulong length ( )
    {
        return this.length_() - this.file_format_version_.sizeof;
    }


    /***************************************************************************

        Returns:
            the number of bytes contained in the file

    ***************************************************************************/

    abstract protected ulong length_ ( );


    /***************************************************************************

        foreach iterator over key/value pairs in the file. The actual reading
        logic depends on the file format version number. See FormatReaderBase
        and derived classes, above.

        Throws:
            if the file format version number is unsupported

    ***************************************************************************/

    public int opApply ( scope int delegate ( ref mstring key, ref mstring value ) dg )
    {
        // Function which instantiates a reader for the appropriate file format
        // version and passes it to the provided delegate. This pattern is used
        // so that the reader can be newed at scope (on the stack).
        void read ( scope void delegate ( FormatReaderBase ) use_reader )
        {
            if ( !this.supported_file_format_version )
            {
                throw new Exception(
                    cast(istring)("Unsupported dump file format: "
                        ~ Integer.toString(this.file_format_version_))
                );
            }

            switch ( this.file_format_version_ )
            {
                case 0:
                    scope v0_reader = new FormatReader_v0;
                    use_reader(v0_reader);
                    break;
                default:
                    enforce(false);
            }
        }

        int res;

        read((FormatReaderBase reader){
            res = reader.opApply(dg);
        });

        return res;
    }


    /***************************************************************************

        Closes the dump file.

    ***************************************************************************/

    public void close ( )
    {
        this.input.close();
    }
}



/*******************************************************************************

    Input buffered direct I/O file dump file reader class.

*******************************************************************************/

public class ChannelLoader : ChannelLoaderBase
{
    /***************************************************************************

        Constructor.

        Params:
            buffer = buffer used by internal direct I/O reader
            disable_direct_io = determines if regular buffered I/O (false) or direct
                I/O is used (true). Regular I/O is only useful for testing,
                because direct I/O imposes some restrictions over the type of
                filesystem that can be used.

    ***************************************************************************/

    public this ( ubyte[] buffer, bool disable_direct_io )
    {
        super(new BufferedDirectReadFile(null, buffer, disable_direct_io));
    }

    /***************************************************************************

        Opens the dump file for reading and reads the file format version number
        at the beginning.

        NOTE: in the old file format, the first 8 bytes actually store the
        number of records contained in the file.

        Params:
            path = path to open

    ***************************************************************************/

    public void open ( cstring path )
    {
        (cast(BufferedDirectReadFile)this.input).open(path);

        super.open();
    }


    /***************************************************************************

        Returns:
            the number of bytes contained in the file

    ***************************************************************************/

    override protected ulong length_ ( )
    {
        return (cast(File)this.input.conduit).length;
    }
}

