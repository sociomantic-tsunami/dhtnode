/*******************************************************************************

    Class to manage the config parameters which specify the hash range of the
    node.

    The hash range takes one of two forms:
        1. The standard form. Min hash <= max hash.
        2. Empty. Min hash and max hash both have magic values (see HashRange),
           allowing this state to be distinguished.

    The empty state is supported to allow new nodes to be started up with no
    current hash responsibility, awaiting an external command to tell them which
    range they should support. It could also be used to effectively delete a
    node by setting its hash range to empty.

    copyright:
        Copyright (c) 2014-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.config.HashRangeConfig;



/*******************************************************************************

    Imports

*******************************************************************************/

import ocean.transition;

import dhtnode.config.ServerConfig;

import swarm.util.Hash : HashRange;

import ocean.core.Array : startsWith;
import ocean.core.Enforce;
import ocean.core.Verify;

import Hash = ocean.text.convert.Hash;

import ocean.text.util.StringSearch;

import ocean.io.device.File;

import ocean.text.Util : locate, trim;

import ocean.util.log.Logger;

version ( UnitTest )
{
    import ocean.core.Test;
}


public class HashRangeConfig
{
    /***************************************************************************

        Static logger.

    ***************************************************************************/

    private static Logger log;
    static this ( )
    {
        log = Log.lookup("dhtnode.config.HashRangeConfig");
    }


    /***************************************************************************

        List of config files which need to be scanned and updated when hash
        range changes (see set()).

    ***************************************************************************/

    private Const!(mstring)[] config_files;

    /***************************************************************************

        Buffer used to read config files into memory. We're assuming that config
        files are small and thus safe to handle in this way.

    ***************************************************************************/

    private mstring file_buf;


    /***************************************************************************

        Buffer used when splitting file_buf by lines.

    ***************************************************************************/

    private mstring[] slices;


    /***************************************************************************

        Buffer used to render a hash_t to mstring.

    ***************************************************************************/

    private mstring hash_buf;


    /***************************************************************************

        Constructor.

        Params:
            config_files = list of config files which need to be scanned and
                updated when hash range changes (see modify())

    ***************************************************************************/

    public this ( in cstring[] config_files )
    {
        this.config_files = config_files;
    }


    /***************************************************************************

        Sets the hash range in the config file(s).

        Params:
            min = min hash
            max = max hash

        Throws:
            if the specified range is invalid

    ***************************************************************************/

    public void set ( hash_t min, hash_t max )
    {
        auto range = HashRange(min, max);
        enforce(range.is_valid && !range.is_empty,
            "Invalid hash range: min must be less than or equal to max");

        this.modify(range);
    }


    /***************************************************************************

        Sets the hash range in the config file(s) to empty.

    ***************************************************************************/

    public void clear ( )
    {
        HashRange empty;
        this.modify(empty);
    }


    /***************************************************************************

        Sets the hash range and updates the config file(s).

        Params:
            range = new hash range to store in config file(s).

    ***************************************************************************/

    private void modify ( HashRange range )
    {
        verify(range.is_valid);

        foreach ( filename; this.config_files )
        {
            scope file = new File(filename, File.ReadWriteExisting);
            this.file_buf.length = file.length;
            enableStomping(this.file_buf);
            file.read(this.file_buf);

            this.updateConfigValue(this.file_buf, "Server", "minval",
                Hash.toHashDigest(range.min, this.hash_buf), this.slices);

            this.updateConfigValue(this.file_buf, "Server", "maxval",
                Hash.toHashDigest(range.max, this.hash_buf), this.slices);

            file.seek(0);
            file.write(this.file_buf);
        }
    }


    /***************************************************************************

        Updates the specified value in the in-memory content of a config file.

        Note: this is not a generic config value updater. It is specifically
        designed to handle hash values with the following idiosyncrasies:
            * specialised logic for stripping 0x radix specifiers.
            * the length of the new value and the old value must match (this is
              true for hex digests and allows a simplistic in-place update).

        Params:
            file = buffer containing the content of the file to be updated
            section = name of config section containing value to be updated
            key = key of value to be updated
            new_val = new value to be written
            slices = buffer to hold slices to the file, split into lines

    ***************************************************************************/

    private static void updateConfigValue ( mstring file, cstring section,
        cstring key, cstring new_val, ref mstring[] slices )
    {
        bool in_section;
        foreach ( line; StringSearch!().split(slices, file, '\n') )
        {
            auto trimmed = trim(line);

            // skip empty lines
            if ( !trimmed.length ) continue;

            // skip comments
            if ( trimmed.startsWith("//") || trimmed.startsWith(";")
              || trimmed.startsWith("#") ) continue;

            // section start
            if ( trimmed[0] == '[' && trimmed[$-1] == ']' )
            {
                in_section = trimmed[1..$-1] == section;
            }
            // value
            else
            {
                // skip values in other sections
                if ( !in_section ) continue;

                // skip wrong keys
                if ( !trimmed.startsWith(key) ) continue;

                // assure there's an = sign in the line
                auto equals = line.locate('=');
                if ( equals >= line.length )
                {
                    log.warn("invalid key/value in config file: '=' missing");
                    continue;
                }

                auto value = trim(line[equals+1..$]);
                if ( value.startsWith("0x") ) // TODO: replace with removePrefix()
                {
                    value = value[2..$];
                }

                // assure the length of the old value == the length of the new
                // value (allowing us to in-place modify)
                if ( value.length != new_val.length )
                {
                    log.warn("cannot modify config value: lengths do not match");
                    continue;
                }

                value[] = new_val[];
            }
        }
    }

    unittest
    {
        void testConfig ( cstring original, cstring section, cstring key,
            cstring new_val, cstring expected )
        {
            mstring[] slices;
            auto file = original.dup;
            HashRangeConfig.updateConfigValue(file, section, key, new_val, slices);

            test!("==")(file, expected,
                "updated config does not match expected content");
        }

        static immutable original =
            "[Server]\nminval = 0x0000000000000000\nmaxval = 0xffffffffffffffff";

        // Non-changes
        testConfig(original, "ServerX", "minval", "1234567812345678", original);
        testConfig(original, "Server", "minvalX", "1234567812345678", original);
        testConfig(original, "Server", "minval", "1234567812345678X", original);

        // Successful changes
        testConfig(original, "Server", "minval", "1234567812345678",
            "[Server]\nminval = 0x1234567812345678\nmaxval = 0xffffffffffffffff");
        testConfig(original, "Server", "maxval", "8765432187654321",
            "[Server]\nminval = 0x0000000000000000\nmaxval = 0x8765432187654321");
    }
}

