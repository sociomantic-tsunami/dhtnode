/*******************************************************************************

    Direct I/O temporary file output

    copyright:
        Copyright (c) 2014-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.storage.DirectIO;

/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import ocean.core.Array : concat;

import ocean.io.device.DirectIO;

import core.sys.posix.unistd : unlink;

import core.stdc.errno : errno, ENOENT;

import ocean.stdc.posix.fcntl : open, O_DIRECT; // O_DIRECT is Linux only

import ocean.util.log.Logger;

/*******************************************************************************

    Static module logger

*******************************************************************************/

private Logger log;
static this ()
{
    log = Log.lookup("dhtnode.storage.DirectIO");
}

/*******************************************************************************

    System calls. (Not defined in tango.)

*******************************************************************************/

extern (C)
{
    int mkostemps (char* path_template, int suffixlen, int flags);
}

/*******************************************************************************

    Buffered file to do direct I/O writes to a temporary file. The temporary
    file is created using the linux mkostemp() function, which appends a unique
    6-character string to the end of the specified path.

    Please read the module documentation for details.

*******************************************************************************/

public class BufferedDirectWriteTempFile : BufferedDirectWriteFile
{
    /***************************************************************************

        Temp file to do direct IO writes.

    ***************************************************************************/

    protected class DirectWriteTempFile : DirectWriteFile
    {
        import ocean.transition;

        /***********************************************************************

            String which contains name of temporary file (filled in by the call
            to mkostemp(), in open()).

            Ideally this would be stored directly in super.path_ (see File), but
            that field is private and has no setter, beyond calling open(),
            which we're explicitly avoiding here.

        ***********************************************************************/

        private mstring temp_file_path;

        /***********************************************************************

            Opens a temporary file at the specified path. The file will have a
            unique 6-character string appended to the path and after that, the
            suffix provided by the outer class.

            Params:
                path = path at which to create temporary file

            Throws:
                IOException on error opening the file

        ***********************************************************************/

        public override void open (cstring path, Style style = this.WriteCreate)
        {
            this.temp_file_path.concat(path, "XXXXXX", this.outer.suffix, "\0");
            auto fd = mkostemps(this.temp_file_path.ptr,
                    cast(int) this.outer.suffix.length,
                    this.outer.disable_direct_io ? 0 : O_DIRECT);
            if (fd == -1)
            {
                // If mkostemp() fails, it might leave the file created
                // afterall, apparently the file is created first and just
                // afterwards O_DIRECT is set and fails.
                // scope (exit) is used so we don't interfere with errno before
                // calling this.error();
                scope (exit)
                {
                    auto r = unlink(this.temp_file_path.ptr);
                    // We ignore NOENT errors as we have nothing to do if the
                    // file wasn't created after all
                    if (r == -1 && errno != ENOENT)
                        log.error("Can't remove failed temporary file {}",
                                this.temp_file_path[0 .. $ - 1]);
                }
                this.error(); // throws an IOException
            }

            // the oddly-named 'reopen' allows us to set the Device's fd
            this.reopen(cast(Handle) fd);
        }

        /***********************************************************************

            Returns:
                the file's path

        ***********************************************************************/

        public override cstring path ()
        {
            return this.temp_file_path;
        }
    }

    /***************************************************************************

        Suffix part of the file name. Is assumed to never change.

    ***************************************************************************/

    private cstring suffix;

    /***************************************************************************

        Determines if regular buffered I/O (true) or direct I/O is used (false).

        This option seems to be completely pointless in this class, and even
        contradictory. The whole point of having this is for testing, using
        direct I/O imposes some restrictions over the type of filesystem that
        can be used that complicates the whole procedure, so by just changing
        the O_DIRECT flag here, we make sure the code path is tested as much as
        possible even when we can't use direct I/O for testing purposes.

    ***************************************************************************/

    private bool disable_direct_io;

    /***************************************************************************

        Constructs a new BufferedDirectWriteTempFile.

        If a path is specified, a temporary file is opened. The name of the
        temporary file is composed of a base path and a suffix. Between both
        there is a random string consisting of 6 lettters to make the name
        unique (this is picked up by mkostemps(3), read the man page for
        details). If path is null, the object is created but no file is opened.
        The suffix is saved for later use, so when opening the file later the
        name will be built as explained before, combining the path, a random
        component and the suffix.

        See documentation for super(char[], ubyte[]) for details on the buffers
        and restrictions.

        Params:
            path = Base path part of the temporary file name.
            buffer = Buffer to use for writing, the length must be multiple of
                     the BLOCK_SIZE and the memory must be aligned to the
                     BLOCK_SIZE
            suffix = Suffix part of the temporary file name.
            disable_direct_io = determines if regular buffered I/O (true) or
                     direct I/O is used (false). This is for testing only,
                     please refer to the documentation for the disable_direct_io
                     attribute for details.

    ***************************************************************************/

    public this (cstring path, ubyte[] buffer, cstring suffix = ".tmp",
            bool disable_direct_io = false)
    {
        this.suffix = suffix;
        this.disable_direct_io = disable_direct_io;
        super(path, buffer);
    }

    /***************************************************************************

        Constructs a new BufferedDirectWriteTempFile allocating a new buffer.

        If a path is specified, a temporary file is opened. The name of the
        temporary file is composed of a base path and a suffix. Between both
        there is a random string consisting of 6 lettters to make the name
        unique (this is picked up by mkostemps(3), read the man page for
        details). If path is null, the object is created but no file is opened.
        The suffix is saved for later use, so when opening the file later the
        name will be built as explained before, combining the path, a random
        component and the suffix.

        See documentation for super(char[], ubyte[]) for details on the buffers
        and restrictions.

        Params:
            path = Path of the file to write to.
            buffer_blocks = Buffer size in blocks (default 32MiB)
            suffix = Suffix part of the temporary file name.
            disable_direct_io = determines if regular buffered I/O (true) or
                    direct I/O is used (false). This is for testing only, please
                    refer to the documentation for the disable_direct_io
                    attribute for details.

    ***************************************************************************/

    public this (cstring path = null, size_t buffer_blocks = 32 * 2 * 1024,
            cstring suffix = ".tmp", bool disable_direct_io = false)
    {
        this.suffix = suffix;
        this.disable_direct_io = disable_direct_io;
        super(path, buffer_blocks);
    }

    /***************************************************************************

        Instantiates the file object to be used to write to. Overrides the base
        class' implementation to return a temp file instead.

        Returns:
            file object to write to

    ***************************************************************************/

    protected override DirectWriteFile newFile ()
    {
        return new DirectWriteTempFile;
    }
}

/*******************************************************************************

    Buffered file to do direct I/O reads, maybe. Just for testing, sometimes we
    need to make this class actually read using non-direct I/O, even when the
    whole point of it is to use direct I/O. This is just for testing purposes
    though. Please read the comment in
    BufferedDirectWriteTempFile.disable_direct_io for more information.

*******************************************************************************/

public class BufferedDirectReadFile : ocean.io.device.DirectIO
    .BufferedDirectReadFile
{
    /***************************************************************************

        Temp file to do direct IO reads.

    ***************************************************************************/

    protected class MaybeDirectReadFile : DirectReadFile
    {
        import ocean.transition;

        /***********************************************************************

            String which contains name of temporary file (filled in by the call
            to mkostemp(), in open()).

            Ideally this would be stored directly in super.path_ (see File), but
            that field is private and has no setter, beyond calling open(),
            which we're explicitly avoiding here.

        ***********************************************************************/

        private mstring temp_file_path;

        /***********************************************************************

            Opens a temporary file at the specified path. The file will have a
            unique 6-character string appended to the path.

            Params:
                path = path at which to create temporary file

            Throws:
                IOException on error opening the file

        ***********************************************************************/

        public override void open (cstring path, Style style = this.ReadExisting)
        {
            this.temp_file_path.concat(path, "\0");
            auto fd = .open(this.temp_file_path.ptr, this.outer.disable_direct_io ? 0 : O_DIRECT);
            if (fd == -1)
            {
                this.error(); // throws an IOException
            }

            // the oddly-named 'reopen' allows us to set the Device's fd
            this.reopen(cast(Handle) fd);
        }
    }

    /***************************************************************************

        Determines if regular buffered I/O (true) or direct I/O is used (false).

        Please read the comment in BufferedDirectWriteTempFile.disable_direct_io
        for more information.

    ***************************************************************************/

    private bool disable_direct_io;

    /***************************************************************************

        Constructs a new BufferedDirectReadFile.

        See notes in BufferedDirectWriteFile about the default buffer size.

        Params:
            path = Path of the file to read from.
            buffer = Buffer to use for reading, the length must be multiple of
                     the BLOCK_SIZE and the memory must be aligned to the
                     BLOCK_SIZE
            disable_direct_io = determines if regular buffered I/O (true) or
                     direct I/O is used (false). This is for testing only,
                     please refer to the documentation for the disable_direct_io
                     attribute for details.

    ***************************************************************************/

    public this (cstring path, ubyte[] buffer, bool disable_direct_io = false)
    {
        this.disable_direct_io = disable_direct_io;
        super(path, buffer);
    }

    /***************************************************************************

        Constructs a new BufferedDirectReadFile allocating a new buffer.

        See documentation for this(char[], ubyte[]) for details.

        Params:
            path = Path of the file to read from.
            buffer_blocks = Buffer size in blocks (default 32MiB)
            disable_direct_io = determines if regular buffered I/O (true) or
                    direct I/O is used (false). This is for testing only, please
                    refer to the documentation for the disable_direct_io
                    attribute for details.

    ***************************************************************************/

    public this (cstring path = null, size_t buffer_blocks = 32 * 2 * 1024,
            bool disable_direct_io = false)
    {
        this.disable_direct_io = disable_direct_io;
        super(path, buffer_blocks);
    }

    /***************************************************************************

        Instantiates the file object to be used to read from, using direct I/O
        or not depending on disable_direct_io.

    ***************************************************************************/

    protected override DirectReadFile newFile ()
    {
        return new MaybeDirectReadFile;
    }
}
