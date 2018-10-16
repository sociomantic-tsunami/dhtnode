export ASSERT_ON_STOMPING_PREVENTION=1

ifneq ($(DVER),1)
	DC := dmd-transitional

	ifeq ($F, production)
		override DFLAGS += -release
	endif
endif

override LDFLAGS += -llzo2
override DFLAGS += -w

ifeq ($(DVER),1)
override DFLAGS += -v2 -v2=-static-arr-params -v2=-volatile
endif

$B/dhtnode: override LDFLAGS += -ltokyocabinet -lebtree -lrt -lgcrypt -lgpg-error -lglib-2.0 -lpcre
$B/dhtnode: src/dhtnode/main.d
dhtnode: $B/dhtnode
all += $B/dhtnode

$B/dhtdump: override LDFLAGS += -lebtree -lrt -lgcrypt -lgpg-error -lglib-2.0 -lpcre
$B/dhtdump: src/dhtnode/dhtdump/main.d
dhtdump: $B/dhtdump
all += $B/dhtdump

$B/dhtredist: override LDFLAGS += -lebtree -lrt -lgcrypt -lgpg-error -lglib-2.0 -lpcre
$B/dhtredist: src/dhtredist/main.d
dhtredist: $B/dhtredist
all += $B/dhtredist

$B/tcmcli: override LDFLAGS += -lebtree -lrt -lgcrypt -lgpg-error -lglib-2.0 -lpcre
$B/tcmcli: src/tcmcli/main.d
tcmcli: $B/tcmcli
all += $B/tcmcli

$B/dhtperformance: override LDFLAGS += -lebtree -lrt -lgcrypt -lgpg-error -lglib-2.0 -lpcre
$B/dhtperformance: src/dhtperformance/main.d
dhtperformance: $B/dhtperformance
all += $B/dhtperformance

$O/test-dhttest: $B/dhtnode
$O/test-dhttest: override LDFLAGS += -lebtree -lrt -lgcrypt -lgpg-error -lglib-2.0 -lpcre

$B/neotest: override LDFLAGS += -lebtree -lrt -lgcrypt -lgpg-error -lglib-2.0 -lpcre
$B/neotest: neotest/main.d
neotest: $B/neotest
all += $B/neotest

# any text passed via TURTLE_ARGS will be used as extra CLI arguments:
#     make run-dhttest TURTLE_ARGS="--help"
#     make run-dhttest TURTLE_ARGS="--id=7"
run-dhttest: $O/test-dhttest $B/dhtnode
	$(call exec, $O/test-dhttest $(TURTLE_ARGS))

debug-dhttest: $O/test-dhttest $B/dhtnode
	$(call exec, gdb --args $O/test-dhttest $(TURTLE_ARGS))

# Additional flags needed when unittesting
$O/%unittests: override LDFLAGS += -ltokyocabinet -lebtree -lrt -lgcrypt -lgpg-error -lglib-2.0 -lpcre

# Packages dependencies
$O/pkg-dhtnode-common.stamp: \
		$(PKG)/defaults.py \
		$C/deploy/logrotate/dhtnode-logs
$O/pkg-dhtnode.stamp: \
		$(PKG)/defaults.py \
		$(PKG)/after_dhtnode_install.sh \
		$B/dhtnode \
		$B/dhtdump
$O/pkg-dhtnode-utils.stamp: \
		$(PKG)/defaults.py \
		$B/tcmcli \
		$B/dhtredist \
		$B/dhtperformance
