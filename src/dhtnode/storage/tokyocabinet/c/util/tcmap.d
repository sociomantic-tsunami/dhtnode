/*******************************************************************************

    D binding for Tokyo Cabinet's tcmap.

    Binding for Tokyo Cabinet map implementation (part of tcutil).
    See http://fallabs.com/tokyocabinet/

    Tokyo Cabinet is copyright (C) 2006-2011 Fal Labs
    copyright:
        Copyright (c) 2009-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.storage.tokyocabinet.c.util.tcmap;

import dhtnode.storage.tokyocabinet.c.util.tclist: TCLIST;

extern (C):

struct TCMAP
{
    TCMAPREC **buckets;
    TCMAPREC *first;
    TCMAPREC *last;
    TCMAPREC *cur;
    uint  bnum;
    ulong rnum;
    ulong msiz;
};

alias void* function (void* vbuf, int vsiz, int* sp, void* op) TCPDPROC;

struct _TCMAPREC
{
    int ksiz;
    int vsiz;
    _TCMAPREC* left;
    _TCMAPREC* right;
    _TCMAPREC* prev;
    _TCMAPREC* next;
};

alias _TCMAPREC TCMAPREC;

TCMAP* tcmapnew();

TCMAP* tcmapnew2(uint bnum);

TCMAP* tcmapnew3(char* str, ...);

TCMAP* tcmapdup(TCMAP* map);

void tcmapdel(TCMAP* map);

void tcmapput(TCMAP* map, void* kbuf, int ksiz, void* vbuf, int vsiz);

void tcmapput2(TCMAP* map, char* kstr, char* vstr);

bool tcmapputkeep(TCMAP* map, void* kbuf, int ksiz, void* vbuf, int vsiz);

bool tcmapputkeep2(TCMAP* map, char* kstr, char* vstr);

void tcmapputcat(TCMAP* map, void* kbuf, int ksiz, void* vbuf, int vsiz);

void tcmapputcat2(TCMAP* map, char* kstr, char* vstr);

bool tcmapout(TCMAP* map, void* kbuf, int ksiz);

bool tcmapout2(TCMAP* map, char* kstr);

void* tcmapget(TCMAP* map, void* kbuf, int ksiz, int* sp);

char* tcmapget2(TCMAP* map, char* kstr);

bool tcmapmove(TCMAP* map, void* kbuf, int ksiz, bool head);

bool tcmapmove2(TCMAP* map, char* kstr, bool head);

void tcmapiterinit(TCMAP* map);

void* tcmapiternext(TCMAP* map, int* sp);

char* tcmapiternext2(TCMAP* map);

ulong tcmaprnum(TCMAP* map);

ulong tcmapmsiz(TCMAP* map);

TCLIST *tcmapkeys(TCMAP* map);

TCLIST *tcmapvals(TCMAP* map);

int tcmapaddint(TCMAP* map, void* kbuf, int ksiz, int num);

double tcmapadddouble(TCMAP* map, void* kbuf, int ksiz, double num);

void tcmapclear(TCMAP* map);

void tcmapcutfront(TCMAP* map, int num);

void* tcmapdump(TCMAP* map, int* sp);

TCMAP* tcmapload(void* ptr, int size);

void tcmapput3(TCMAP* map, void* kbuf, int ksiz, char* vbuf, int vsiz);

void tcmapput4(TCMAP* map, void* kbuf, int ksiz,
                 void* fvbuf, int fvsiz, void* lvbuf, int lvsiz);

void tcmapputcat3(TCMAP* map, void* kbuf, int ksiz, void* vbuf, int vsiz);

bool tcmapputproc(TCMAP* map, void* kbuf, int ksiz, void* vbuf, int vsiz,
                    scope TCPDPROC proc, void* op);

void* tcmapget3(TCMAP* map, void* kbuf, int ksiz, int* sp);

char* tcmapget4(TCMAP* map, char* kstr, char* dstr);

void tcmapiterinit2(TCMAP* map, void* kbuf, int ksiz);

void tcmapiterinit3(TCMAP* map, char* kstr);

void* tcmapiterval(void* kbuf, int* sp);

char* tcmapiterval2(char* kstr);

char* *tcmapkeys2(TCMAP* map, int* np);

char* *tcmapvals2(TCMAP* map, int* np);

void* tcmaploadone(void* ptr, int size, void* kbuf, int ksiz, int* sp);

void tcmapprintf(TCMAP* map, char* kstr, char* format, ...);
