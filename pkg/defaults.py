OPTS.update(
    name = VAR.fullname,
    url = 'https://github.com/sociomantic/dhtnode',
    maintainer = 'Sociomantic Labs GmbH <tsunami@sociomantic.com>',
    vendor = 'Sociomantic Labs GmbH',
    description = '''Distributed hash-table node
The dht node is a server which handles requests from the dht client defined in
swarm (swarm.dht.DhtClient). One or more nodes make up a complete dht, though
only the client has this knowledge -- individual nodes know nothing of each
others' existence.

Data in the dht node is stored in memory, in instances of the Tokyo Cabinet
memory database, with a separate instance per data channel.''',
)

ARGS.extend([
    "README.rst=/usr/share/doc/{}/".format(VAR.fullname),
])

# vim: set ft=python et sw=4 sts=4 :
