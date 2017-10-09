#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <openssl/sha.h>

#include "table.h"

static int select_item(uint8_t *hash)
{
    uint8_t *start_byte = &(hash[16]);
    uint32_t hv = *((uint32_t *)start_byte);

    return hv % 10;
}

static int select_bucket(uint8_t *hash)
{
    uint8_t *start_byte = &(hash[4]);
    uint32_t hv = *((uint32_t *)start_byte);

    return hv % 10;
}

static Item* keyvalue_to_item(char *key, char *value)
{
    Item *item = (Item *)malloc(sizeof(Item));
    bzero(item, sizeof(*item));

    SHA1((uint8_t *)key, sizeof(key), item->hash);

    memcpy(item->key, key, strlen(key));
    memcpy(item->value, value, strlen(value));

    return item;
}

static inline bool item_identical_key(Item* item, char* key)
{
    return memcmp(item->key, key, strlen(key)) == 0;
}

static void item_insert(Item **items, Item *item)
{
    Item **iter = items;

    while(*iter)
    {   
        if(item_identical_key((*iter), item->key))
        {   
            //replace
            memcpy((*iter)->value, item->value, strlen(item->value));
            
            return;
        }

        iter = &((*iter)->next);
    }

    item->next = *items;
    (*items) = item;
}

static void bucket_insert(Bucket *bucket, Item *item)
{
    int tid = select_item(item->hash);

    Item **items = &(bucket->items[tid]);

    item_insert(items, item);

    bucket->item_count += 1;
}

static void table_insert_item(Table *table, Item *item)
{
    int bid = select_bucket(item->hash);

    Bucket *bucket = table->buckets[bid];
    bucket_insert(bucket, item);

    table->total_count += 1;
}

bool table_full(Table *table)
{
    if(table->total_count == table->cap)
        return true;

    return false;
}

Table* table_malloc()
{
    Table *table = (Table *)malloc(sizeof(Table));
    bzero(table, sizeof(*table));

    for(int i=0; i<10; i++)
    {
        table->buckets[i] = (Bucket *)malloc(sizeof(Bucket));
        bzero(table->buckets[i], sizeof(Bucket));
    }

    table->total_count = 0;
    table->cap = 100;

    return table;
}

bool table_insert_kv(Table* table, char *key, char *value)
{
    if(table_full(table))
        return false;
    
    Item *item = keyvalue_to_item(key, value);

    int bid = select_bucket(item->hash);

    if(item == NULL) return false;

    table_insert_item(table, item);

    return true;
}

static Item* item_lookup(Item *items, char *key)
{
    Item *iter = items;

    while(iter)
    {
        if(item_identical_key(iter, key))
            return iter;

        iter = iter->next;
    }

    return NULL;
}

static Item* bucket_lookup(Bucket *bucket, char* key, uint8_t* hash)
{
    int tid = select_item(hash);

    Item* item = item_lookup(bucket->items[tid], key);

    if(item == NULL) return NULL;

    return item;
}

bool table_lookup(Table* table, char* key, uint8_t* hash)
{
    int bid = select_bucket(hash);

    Bucket* bucket = table->buckets[bid];

    Item* item = bucket_lookup(bucket, key, hash);

    if(item)
    {
        printf("value: %s\n", item->value);

        return true;
    }
    else
        return false;
}

static BloomFliter* table_bulid_bf(Bucket *bucket)
{
    if(bucket->item_count == 0)
        return NULL;

    BloomFliter *bf = (BloomFliter *)malloc(sizeof(BloomFliter));

    for(int i=0; i<10; i++)
    {
        Item *iter = bucket->items[i];

        while(iter)
        {
            bf_add(bf, iter->key);

            iter = iter->next;
        }
    }

    return bf;
}

MTable* table_to_metatable(Table* table)
{
    MTable *mtable = (MTable *)malloc(sizeof(MTable));
    bzero(mtable, sizeof(MTable));

    mtable->table = table;

    for(int i=0; i<10; i++)
        mtable->bfs[i] = table_bulid_bf(table->buckets[i]);

    return mtable;
}

static void raw_item_insert(Item **items, Item *item)
{
    Item *new_item = keyvalue_to_item(item->key, item->value);
    Item **iter = items;

    while(*iter)
    {   
        if(item_identical_key((*iter), new_item->key))
        {   
            //replace
            memcpy((*iter)->value, new_item->value, strlen(new_item->value));
            
            return;
        }

        iter = &((*iter)->next);
    }

    new_item->next = *items;
    (*items) = new_item;
}

static void raw_bucket_insert(Bucket *bucket, Item *item)
{
    int tid = select_item(item->hash);

    Item **items = &(bucket->items[tid]);

    raw_item_insert(items, item);

    bucket->item_count += 1;
}

static void raw_item_feed_to_tables(Item *item, Table* table)
{
    int bid = select_bucket(item->hash);

    Bucket *bucket = table->buckets[bid];
    raw_bucket_insert(bucket, item);

    table->total_count += 1;
}

static void raw_buckets_feed_to_tables(Bucket *bucket, Table* tables[], uint64_t sub_bit, int (* compaction_select)(uint8_t *, uint64_t))
{   
    for(int i=0; i<10; i++)
    {
        Item *item = bucket->items[i];

        if(item)
        {
            Item *iter = item;
            
            while(iter)
            {
                int tid = compaction_select(iter->hash, sub_bit);
                raw_item_feed_to_tables(iter, tables[tid]);

                if(table_full(tables[tid]))
                    return;

                iter = iter->next;
            }
        }
    }
}

bool metatable_feed_to_table(MTable* mtable, Table* tables[], uint64_t sub_bit, int (* compaction_select)(uint8_t *, uint64_t))
{
    Table *t = mtable->table;

    for(int i=0; i<10; i++)
    {
        Bucket *bucket = t->buckets[i];

        raw_buckets_feed_to_tables(bucket, tables, sub_bit, compaction_select);
    }

    return true;
}

bool metatable_lookup(MTable *mtable, char *key, uint8_t* hash)
{
    int bid  = select_bucket(hash);

    BloomFliter* bf = mtable->bfs[bid];

    if(bf)
    {   
        if(bf_match(bf, key) == true)
            return table_lookup(mtable->table, key, hash);
        else
            return false;
    }
}

void meta_table_dump(char* store_path, uint64_t cid, MTable *mtable)
{
    char f_path[4096];
    sprintf(f_path, "%s/%02" PRIx64 "/%016" PRIx64, store_path, cid, mtable->mtid);

    //bulid a file to store the data
    FILE *f = fopen(f_path, "w");  

    Table *table = mtable->table;

    for(int i=0; i<10; i++)
    {
        fprintf(f, "bid %d:\n", i+1);
        Bucket *bucket = table->buckets[i];

        for(int i=0; i<10; i++)
        {
            fprintf(f, "tid %d: ", i+1);
            Item *item = bucket->items[i];

            if(item)
            {
                Item *iter = item;
                
                while(iter)
                {
                    fprintf(f, "key: %s, value: %s ", iter->key, iter->value);

                    iter = iter->next;
                }

                fprintf(f, "\n");
            }
            else
                fprintf(f, "NULL\n");
        }
        fprintf(f, "\n");
    }

    fclose(f);
}

void meta_table_remove(char* store_path, uint64_t cid, uint64_t mtid)
{
    char f_path[4096];
    sprintf(f_path, "%s/%02" PRIx64 "/%016" PRIx64, store_path, cid, mtid);

    //remove table
    remove(f_path);
}

void free_mtable(MTable *mtable)
{
    Table *table = mtable->table;

    for(int i=0; i<10; i++)
    {
        Bucket *bucket = table->buckets[i];

        for(int i=0; i<10; i++)
        {
            Item *item = bucket->items[i];
            Item *iter = item;

            if(item)
            {   
                Item *t;

                while(iter)
                {
                    t = iter;
                    iter = iter->next;
                    free(t);
                }
            }

            bucket->items[i] = iter;
        }

        if(mtable->bfs[i])
            free(mtable->bfs[i]);
    }
}

void show_table(Table *table)
{
    for(int i=0; i<10; i++)
    {
        printf("bid: %d\n", i+1);
        Bucket *bucket = table->buckets[i];

        for(int i=0; i<10; i++)
        {
            printf("tid: %d ", i+1);
            Item *item = bucket->items[i];

            if(item)
            {
                Item *iter = item;
                
                while(iter)
                {
                    printf("key: %s, value: %s ", iter->key, iter->value);

                    iter = iter->next;
                }

                printf("\n");
            }
            else
                printf("NULL\n");
        }
    }
}