#pragma once

#include <stdio.h>
#include <pthread.h>
#include "table.h"

typedef struct Container
{
    int count;
    MTable *mtables[10];
}Container;

struct VirtualContainer
{
    uint64_t vc_id;
    uint64_t start_bit;
    Container cc;
    struct VirtualContainer *sub_vc[4];
};

typedef struct VirtualContainer VirtualContainer;

typedef struct DB
{
    char *store_dir;
    Table* active_table[2];

    pthread_t active_id;
    pthread_t compaction_id;

    pthread_mutex_t mutex_active;
    pthread_mutex_t mutex_compaction;

    pthread_cond_t cond_active;
    pthread_cond_t cond_writer;
    pthread_cond_t cond_producer;
    pthread_cond_t cond_consumer;

    uint64_t mtid_next;
    uint64_t vcid_next;

    bool db_test;

    VirtualContainer *vcroot;

    FILE* log;
}DB;

typedef struct Compaction
{
    uint64_t start_bit;
    uint64_t sub_bit;

    MTable *molds[4];
    Table *news[4];
    MTable *mnews[4];

    uint64_t mtids[4];

    DB *db;
    VirtualContainer *vc;
}Compaction;

DB* db_create();

void db_close(DB* db);

bool Put(DB* db, char* key, char* value);

void Get(DB* db, char* key);
