.. contents::
  :depth: 2

Dht Node
^^^^^^^^

Description
===========

The dht node is a server which handles requests from the dht client defined in
swarm (``swarm.dht.DhtClient``). One or more nodes make up a complete dht,
though only the client has this knowledge -- individual nodes know nothing of
each others' existence.

Data in the dht node is stored in memory, in instances of the `Tokyo Cabinet`__
memory database, with a separate instance per data channel.

__ http://fallabs.com/tokyocabinet/

Deployment
==========

Processes
---------

Many machines run multiple instances of the dhtnode. There should be a directory
in ``/srv/dhtnode/`` for each instance, like ``/srv/dhtnode/dhtnode-XX``. Each
directory should contain:
  * An ``etc/config.ini`` file with the configuration for that instance.
  * In case when using upstart as an init system, a symlink -- named `dhtnode`
    -- to the binary to run (should be one of the binaries located in ``/usr/sbin/``).


Upstart
-------

Two upstart scripts exist: ``/etc/init.dht.conf`` and
``/etc/init/dhtnode-instance.conf``. The latter starts a single instance of the
dhtnode and the former starts all instances configured on the server. Configured
instances are auto-detected by checking for the presence of ``config.ini`` files
in ``/srv/dhtnode/dhtnode-*/etc/``.


Systemd
-------

Two systemd unit templates exist: ``/lib/systemd/system/dht@.service`` and
``/lib/systemd/system/dhtdum@.service``. These files are service templates and
they can be used to start any number of enabled instances on the machine. Enabling
instances is done via ``sudo systemctl enable dht@N`` where ``N`` is the instance
number. To enable first 8 instances, one can use its shell and do
``sudo systemctl enable dht@{1..8}``.

Manually
--------

upstart
.......

To manually start the DHT node(s) on a server, run ``sudo service dht start``.
This will start the processes. If the nodes are already running, you'll need to
shut them down first (``sudo service dht stop``) before restarting them.

An individual dhtnode instance can be started like this:
``sudo start dhtnode-instance INSTANCE=1 CONFIG=/srv/dhtnode/dhtnode-1/etc/config.ini``
and stopped like this:
``sudo stop dhtnode-instance INSTANCE=1``

systemd
.......

To manually start the DHT node(s) on a server, run ``sudo systemctl start dht@{1..n}``.
This will start the first ``n`` enabled instances.. To manually restart or stop
all instances, wild card can be used: ``sudo systemctl {verb} 'dht@*'``, where
``{verb}`` can be either ``stop`` or ``restart``. An individual node can be started,
stopped, or restarted in the same fashion, just specify the instance number after
the ``@`` in the previous commands.

Overriding the binary to run
----------------------------

In case you want to run different binary of dhtnode than the one in
``/usr/sbin/dhtnode``, you can specify that by providing systemd override file.
To do so, create a file ``/etc/systemd/system/dht@1.service.d/override.conf``
with the following content:

.. code:: ini

    [Unit]
    Description=Custom executable image to run
    [Service]
    # Need to clear the previous one first
    ExecPath=
    ExecPath=/tmp/dhtnode-test -c /srv/dhtnode/dhtnode-1/etc/config.ini

Monitoring
==========

Resource Usage
--------------

A dht node process typically uses 40 to 50% CPU usage, and a very high
proportion of the server's RAM (divided between the number of running instances
-- it is expected that all together the dht nodes instances on a single server
should consume up to 90% of ther server's RAM). Anything beyond this might
indicate a problem.

Checking Everything's OK
------------------------

Log Files
.........

The dht node writes two log files:

``root.log``
  Notification of errors when handling requests.

``stats.log``
  Statistics about the number of records and bytes stored (globally and per
  channel), the number of bytes sent and received over the network, and the
  number of open connections and records handled.

Dump Files
..........

The dht node's ``data`` folder should contain one ``.tcm`` file per channel
stored. These are periodically written from the data in memory. When a dump
happens, the old ``.tcm`` file is renamed to ``.tcm.backup``. The ``.tcm`` file
for each channel should have been updated within the last 6 hours.

Systemctl journal
-----------------

To inspect the state of the DHT service, one can use
``sudo systemctl status 'dht@*'``. To see the log, use ``journalctl -u 'dht@*'``.

Possible Problems
-----------------

Crash
.....

Many applications in the system rely on being able to read and/or write to the
dht. If a single dht node goes down, an equivalent proportion of requests from
client applications will fail. There is currently no fall-back mechanism, beyond
the possibility for the client applications themselves to cache and retry failed
requests. The system is, at this stage, pretty robust; all client applications
can handle the situation where a dht node is inaccessible and reconnect safely
when it returns.

If a dht node crashes while in the middle of dumping its memory data to disk,
all that will happen is that a partly-written temporary file will be found on
the disk. This truncated file can be ignored and will not be loaded by the node
upon restart.

Dump Failure
............

There have been instances in the past where the periodic channel dumping stopped
working. Currently, some dht nodes are performing this periodic dumping
themselves, while (a few) others have handed the duty over to the dht dump
process (see below). If dumping stops working, the procedure in each of these
cases is slightly different:

Dht node
  You can try shutting down the node and hope that the dump which is made at
  shutdown will succeed. If this doesn't succeed, then you'll need to look
  through the backup channel dumps to see if you can find any more useful data
  (i.e. larger dump files).

Dht dump process
  You should be able to simply restart the dht dump process, which should
  reconnect to the node and perform a dump.

Design
======

The structure of the nodes' code is based very closely around the structure of
the ``core.node`` package of swarm.

The basic components are:

Select Listener
  The ``swarm.core.node.model.Node : NodeBase`` class, which forms the
  foundation of all swarm nodes, owns an instance of
  ``ocean.net.server.SelectListener : SelectListener``. This provides the basic
  functionality of a server; that is, a listening socket which will accept
  incoming client connections. Each client connection is assigned to a
  connection handler instance from a pool.

Connection Handler Pool
  The select listener manages a pool of connection handlers (derived from
  ``swarm.core.node.connection.ConnectionHandler : ConnectionHandlerTemplate``.
  Each is associated with an incoming socket connection from a client. The
  connection handler reads a request code from the socket and then passes the
  request on to a request handler instance, which is constructed at scope (i.e.
  only exists for the lifetime of the request).

Request Handlers
  A handler class exists for each type of request which the node can handle.
  These are derived from ``swarm.core.node.request.model.IRequest : IRequest``.
  The request handler performs all communication with the client which is
  required by the protocol for the given request. This usually involves
  interacting with the node's storage channels.

Storage Channels
  The ``swarm.core.node.storage.model.IStorageChannels : IStorageChannelsTemplate``
  class provides the base for a set of storage channels, where each channel is
  conceived as storing a different type of data in the system. The individual
  storage channels are derived from
  ``swarm.core.node.storage.model.IStorageEngine : IStorageEngine``.

Data Flow
=========

Dht nodes do not access any other data stores.

Dependencies
============

:Dependency: libtokyocabinet
:Dependency: liblzo2
:Dependency: tango v1.1.5

Dht Dump
^^^^^^^^

Description
===========

The dht dump process is responsible for saving the in-memory dht data to disk in
a location where the dht node can load it upon startup. One dht dump process
runs per dht node process, on the same server. Each dht dump process is thus
responsible for saving the data stored in a single dht node. As the processes
are running on the same server, the data can be transferred locally, without
going through the network interface.

The dump process spends most of its time sleeping, waking up periodically to
read its dht node's data (via GetAll requests to all channels) and write it to
disk. The period and the location to which the dumped data should be written are
set in the config file.

Note: this process is a replacement for the dump thread which exists in the
currently deployed versions of the dht node.

Deployment
==========

Processes
---------

Many machines run multiple instances of dhtdump. There should be a directory
in ``/srv/dhtnode/dhtnode-*`` for each instance, like
``/srv/dhtnode/dhtnode-XX/dump``. Each directory should contain:
  * An ``etc/config.ini`` file with the configuration for that instance.
  * A symlink -- named `dhtdump` -- to the binary to run (should be one of the
    binaries located in ``/usr/sbin/``).

Upstart
-------

The dhtdump processes are configured to use upstart and will start automatically
upon server reboot. The upstart scripts are located in
``etc/init/dhtdump.conf``.

Manually
--------

To manually start the DHT dump process(es) on a server, run
``sudo service dhtdump start``. This will start the processes. If they are
already running, you'll need to shut them down first
(``sudo service dhtdump stop``) before restarting them.

An individual dhtdump instance can be started like this:
``sudo start dhtdump-instance INSTANCE=1 CONFIG=/srv/dhtnode/dhtnode-1/dump/etc/config.ini``

and stopped like this:
``sudo stop dhtdump-instance INSTANCE=1``

Monitoring
==========

Resource Usage
--------------

A dht dump process typically uses around 40-50Mb of memory and 0% CPU when
sleeping.

Checking Everything's OK
------------------------

Console Output
..............

The dht dump process does not, by default make any console output. The deployed
instances are, however, configured to mirror their log output (see below) to the
console.

Log Files
.........

The dht dump process writes two log files:

``root.log``
  Notification of the process' activity. The latest logline will either indicate
  which channel is being dumped to disk or, while the process is sleeping, the
  time at which the next dump cycle is scheduled to begin.

``stats.log``
  Statistics about the number of records and bytes written per log update (every
  30s) and the size of each channel (in terms of records and bytes) the last
  time it was dumped.

Dump Files
..........

The configured dump location should contain one ``.tcm`` file per channel stored
in the dht node. When a dump happens, the old ``.tcm`` file is renamed to
``.tcm.backup``. The ``.tcm`` file for each channel should have been updated
within the period configured in the dump process' config file (typically 6
hours).

Additionally, a cron job runs on the dht servers which makes a daily backup of
the ``.tcm`` files in the ``data`` folder. These backups are zipped and stored
in ``backup-data``.

Possible Problems
-----------------

Crash
.....

If a dht dump process crashes, the world does not end. It can simply be
restarted when it is noticed that it's no longer running.

If a dht dump process crashes while in the middle of dumping its memory data to
disk, all that will happen is that a partly-written temporary file will be found
on the disk. This truncated file can be ignored and will not be loaded by the
dht node if it restarts.

Design
======

Dht dump is a very simple program. It has the following components:

Dump Cycle
  ``dhtnode.dhtdump.DumpCycle``. Manages the process of sleeping and dumping.

Dht Client
  Owned by the dump cycle. Used to contact the dht node and read the stored
  data. (As only a single node is being contacted, we have to cheat and not
  perform the node handshake, which would fail. This is, in practice, ok, as
  only GetChannels and GetAll requests are performed, which are sent to all
  nodes in the client's registry, without a hash responsibility lookup.)

Dump Stats
  ``dhtnode.dhtdump.DumpStats``. Aggregates and logs the stats output by the
  process (see above).

Data Flow
=========

The dht dump process accesses all channels in a single dht node, which should be
running on the same server.

Dependencies
============

:Dependency: libebtree
:Dependency: liblzo2
