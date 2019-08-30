/*******************************************************************************

    D binding for Tokyo Cabinet's tclist.

    Binding for Tokyo Cabinet list implementation (part of tcutil).
    See http://fallabs.com/tokyocabinet/

    Tokyo Cabinet is copyright (C) 2006-2011 Fal Labs

    copyright:
        Copyright (c) 2009-2017 dunnhumby Germany GmbH. All rights reserved

    License:
        Boost Software License Version 1.0. See LICENSE.txt for details.

*******************************************************************************/

module dhtnode.storage.tokyocabinet.c.util.tclist;

extern (C):

struct TCLISTDATUM
{
    char*   ptr;
    int     size;
};

struct TCLIST
{
    TCLISTDATUM* array;
    int          anum;
    int          start;
    int          num;
};

alias int function (TCLISTDATUM*, TCLISTDATUM*) ListCmp;

TCLIST* tclistnew();

TCLIST* tclistnew2(int anum);

TCLIST* tclistnew3(char* str, ...);

TCLIST* tclistdup(TCLIST* list);

void tclistdel(TCLIST* list);

int tclistnum(TCLIST* list);

void* tclistval(TCLIST* list, int index, int* sp);

char* tclistval2(TCLIST* list, int index);

void tclistpush(TCLIST* list, void* ptr, int size);

void tclistpush2(TCLIST* list, char* str);

void* tclistpop(TCLIST* list, int* sp);

char* tclistpop2(TCLIST* list);

void tclistunshift(TCLIST* list, void* ptr, int size);

void tclistunshift2(TCLIST* list, char* str);

void* tclistshift(TCLIST* list, int* sp);

char* tclistshift2(TCLIST* list);

void tclistinsert(TCLIST* list, int index, void* ptr, int size);

void tclistinsert2(TCLIST* list, int index, char* str);

void* tclistremove(TCLIST* list, int index, int* sp);

char* tclistremove2(TCLIST* list, int index);

void tclistover(TCLIST* list, int index, void* ptr, int size);

void tclistover2(TCLIST* list, int index, char* str);

void tclistsort(TCLIST* list);

int tclistlsearch(TCLIST* list, void* ptr, int size);

int tclistbsearch(TCLIST* list, void* ptr, int size);

void tclistclear(TCLIST* list);

void* tclistdump(TCLIST* list, int* sp);

TCLIST* tclistload(void* ptr, int size);

void tclistpushmalloc(TCLIST* list, void* ptr, int size);

void tclistsortci(TCLIST* list);

void tclistsortex(TCLIST* list, scope ListCmp cmp);

void tclistinvert(TCLIST* list);

void tclistprintf(TCLIST* list, char* format, ...);
