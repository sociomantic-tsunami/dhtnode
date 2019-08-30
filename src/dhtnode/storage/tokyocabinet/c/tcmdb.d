/*******************************************************************************

    D binding for Tokyo Cabinet's tcmdb.

    Binding for Tokyo Cabinet memory hash database implementation.
    See http://fallabs.com/tokyocabinet/

    Tokyo Cabinet is copyright (C) 2006-2011 Fal Labs

    copyright:
        Copyright (c) 2009-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.storage.tokyocabinet.c.tcmdb;

import dhtnode.storage.tokyocabinet.c.util.tclist: TCLIST;
import dhtnode.storage.tokyocabinet.c.util.tcmap:  TCMAP, TCPDPROC;

extern (C):

alias bool function (void* kbuf, int ksiz, void* vbuf, int vsiz, void* op) TCITER;

struct TCMDB
{
    void**  mmtxs;
    void*   imtx;
    TCMAP** maps;
    int iter;
};

TCMDB* tcmdbnew();

TCMDB* tcmdbnew2(uint bnum);

void tcmdbdel(TCMDB* mdb);

void tcmdbput(TCMDB* mdb, in void* kbuf, int ksiz, in void* vbuf, int vsiz);

void tcmdbput2(TCMDB* mdb, char* kstr, char* vstr);

bool tcmdbputkeep(TCMDB* mdb, void* kbuf, int ksiz, void* vbuf, int vsiz);

bool tcmdbputkeep2(TCMDB* mdb, char* kstr, char* vstr);

void tcmdbputcat(TCMDB* mdb, void* kbuf, int ksiz, void* vbuf, int vsiz);

void tcmdbputcat2(TCMDB* mdb, char* kstr, char* vstr);

bool tcmdbout(TCMDB* mdb, in void* kbuf, int ksiz);

bool tcmdbout2(TCMDB* mdb, char* kstr);

void* tcmdbget(TCMDB* mdb, in void* kbuf, int ksiz, int* sp);

char* tcmdbget2(TCMDB* mdb, in char* kstr);

int tcmdbvsiz(TCMDB* mdb, in void* kbuf, int ksiz);

int tcmdbvsiz2(TCMDB* mdb, char* kstr);

void tcmdbiterinit(TCMDB* mdb);

void* tcmdbiternext(TCMDB* mdb, int* sp);

char* tcmdbiternext2(TCMDB* mdb);

TCLIST *tcmdbfwmkeys(TCMDB* mdb, void* pbuf, int psiz, int max);

TCLIST *tcmdbfwmkeys2(TCMDB* mdb, char* pstr, int max);

ulong tcmdbrnum(TCMDB* mdb);

ulong tcmdbmsiz(TCMDB* mdb);

int tcmdbaddint(TCMDB* mdb, void* kbuf, int ksiz, int num);

double tcmdbadddouble(TCMDB* mdb, void* kbuf, int ksiz, double num);

void tcmdbvanish(TCMDB* mdb);

void tcmdbcutfront(TCMDB* mdb, int num);


void tcmdbput3(TCMDB* mdb, void* kbuf, int ksiz, char* vbuf, int vsiz);

void tcmdbput4(TCMDB* mdb, void* kbuf, int ksiz,
               void* fvbuf, int fvsiz, void* lvbuf, int lvsiz);

void tcmdbputcat3(TCMDB* mdb, void* kbuf, int ksiz, void* vbuf, int vsiz);

bool tcmdbputproc(TCMDB* mdb, void* kbuf, int ksiz, void* vbuf, int vsiz,
                  scope TCPDPROC proc, void* op);

void* tcmdbget3(TCMDB* mdb, in void* kbuf, int ksiz, int* sp);

void tcmdbiterinit2(TCMDB* mdb, in void* kbuf, int ksiz);

void tcmdbiterinit3(TCMDB* mdb, char* kstr);

void tcmdbforeach(TCMDB* mdb, scope TCITER iter, void* op);
