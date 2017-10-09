#pragma once

#include <stdio.h>
#include <inttypes.h>
#include <stdbool.h>

#include "bloom_fliter.h"

struct Item
{
    char key[160];
    char value[1024];
    uint8_t hash[20];
    struct Item* next;
};

typedef struct Item Item;

typedef struct Bucket
{
    int item_count;
    Item *items[10];
}Bucket;

typedef struct Table
{ 
    int total_count;
    int cap; 
    Bucket *buckets[10];
}Table;

typedef struct MetaTable
{   
    uint64_t mtid;
    Table *table;
    BloomFliter* bfs[10];
}MTable;

bool table_full(Table* table);

Table* table_malloc();

bool table_insert_kv(Table* table, char* key, char* value);

bool table_lookup(Table* table, char* key, uint8_t* hash);

void free_mtable(MTable *mtable);

void show_table(Table *table);

//----------MetaTable------------
MTable* table_to_metatable(Table* table);

void meta_table_dump(char *store_path, uint64_t cid, MTable *mtable);

bool metatable_feed_to_table(MTable* mtable, Table* tables[], uint64_t sub_bit, int (* compaction_select)(uint8_t *, uint64_t));

bool metatable_lookup(MTable *mtable, char* key, uint8_t* hash);

void meta_table_remove(char* store_path, uint64_t cid, uint64_t mtid);
