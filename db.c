#define _GNU_SOURCE

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/stat.h> 
#include <assert.h>
#include <openssl/sha.h>

#include "db.h"
#include "log.h"
#include "table.h"

static VirtualContainer *vc_create(uint64_t start_bit)
{
    VirtualContainer *vc = (VirtualContainer *)malloc(sizeof(VirtualContainer));
    bzero(vc, sizeof(vc));
    vc->start_bit = start_bit;

    return vc;
} 

static bool vc_insert(VirtualContainer* vc, MTable* mtable)
{
    if(vc->cc.count < 10)
    {
        int id = vc->cc.count;

        vc->cc.mtables[id] = mtable;

        vc->cc.count++;
    }
    else
        return false;
}

static int vc_count(DB* db)
{
    if(db->vcroot->cc.count >= 4)
        return 4;

    return 0;
}

static int compaction_select(uint8_t* hash, uint64_t start_bit)
{
    uint64_t select_bit = start_bit - 3;
    uint8_t *start_byte = hash + select_bit;
    uint32_t hv = *((uint8_t *)start_byte);

    return hv % 4;
}

static void compaction_init(DB* db, VirtualContainer* vc, Compaction* comp)
{
    bzero(comp, sizeof(*comp));

    comp->start_bit = vc->start_bit;
    comp->sub_bit = vc->start_bit + 3;
    comp->db = db;
    comp->vc = vc;

    for(int i=0; i<4; i++)
        comp->molds[i] = vc->cc.mtables[i];

    //log the odl_mtid
    for(int i=0; i<4; i++)
        comp->mtids[i] = vc->cc.mtables[i]->mtid;

    for(int i=0; i<4; i++)
        comp->news[i] = table_malloc();

    for(int i=0; i<4; i++)
    {
        if(!comp->vc->sub_vc[i])
        {
            comp->vc->sub_vc[i] = vc_create(comp->sub_bit);
            comp->vc->sub_vc[i]->vc_id = db->vcid_next;
            db->vcid_next++;
        }
    }
}

static void compaction_feed(MTable* mt, Compaction* comp)
{
    metatable_feed_to_table(mt, comp->news, comp->sub_bit, compaction_select);
}

static void compaction_feed_all(Compaction* comp)
{
    for(int i=0; i<4; i++)
    {   
        MTable *mt = comp->molds[i];

        compaction_feed(mt, comp);
    }
}

static void compaction_bulid_bf(Compaction* comp)
{   
    for(int i=0; i<4; i++)
    {
        comp->mnews[i] = table_to_metatable(comp->news[i]);
        comp->mnews[i]->mtid = comp->mtids[i];
    }
}

static void compaction_vc_update(Compaction* comp)
{
    VirtualContainer *vc = comp->vc;

    for(int i=0; i<4; i++)
        vc_insert(vc->sub_vc[i], comp->mnews[i]);

    //dump the table
    for(uint64_t i=0; i<4; i++)
        meta_table_dump(comp->db->store_dir, 4 * vc->vc_id + i + 1, comp->mnews[i]);

    int nr_rest = vc->cc.count - 4;

    //shift
    for(int i=0; i<nr_rest; i++)
    {
        vc->cc.mtables[i] = vc->cc.mtables[i + 4];
        vc->cc.mtables[i + 4] = NULL;
    }

    vc->cc.count = nr_rest;
}

static void compaction_free(Compaction* comp)
{
    for(int i=0; i<4; i++)
    {
        free_mtable(comp->molds[i]);
        meta_table_remove(comp->db->store_dir, comp->vc->vc_id, comp->mtids[i]);
    }
}

void compaction_main(DB* db, VirtualContainer *vc)
{
    Compaction comp;

    compaction_init(db, vc, &comp);

    compaction_feed_all(&comp);
    
    compaction_bulid_bf(&comp);
    
    compaction_vc_update(&comp);
    
    compaction_free(&comp);

    for(int i=0; i<4; i++)
    {
        if(vc->sub_vc[i] && vc->sub_vc[i]->cc.count >= 4)
            compaction_main(db, vc->sub_vc[i]);
    }

    pthread_mutex_lock(&(db->mutex_compaction));  
    pthread_cond_signal(&(db->cond_producer));
    pthread_mutex_unlock(&(db->mutex_compaction));
}

void *thread_compaction(void* arg)
{
    while(true)
    {
        DB* db = (DB *)arg;

        pthread_mutex_lock(&(db->mutex_compaction));
        while(vc_count(db) == 0)
        {
            pthread_cond_wait(&(db->cond_consumer), &(db->mutex_compaction));
        }
        pthread_mutex_unlock(&(db->mutex_compaction));

        db_log(db, "begin compaction");

        compaction_main(db, db->vcroot);

        db_log(db, "compaction successfully");
    }

    pthread_exit(0);
}

void *active_dumper(void* arg)
{
    while(true)
    {
        DB* db = (DB *)arg;

        pthread_mutex_lock(&(db->mutex_active));
        while(table_full(db->active_table[0]) == false)
        {
            pthread_cond_wait(&(db->cond_active), &(db->mutex_active));
        }

        db->active_table[1] = db->active_table[0];
        db->active_table[0] = table_malloc();

        pthread_cond_signal(&(db->cond_writer));
        pthread_mutex_unlock(&(db->mutex_active));

        pthread_mutex_lock(&(db->mutex_compaction));
        while(db->vcroot->cc.count == 10)
        {
            pthread_cond_wait(&(db->cond_producer), &(db->mutex_compaction));
        }
        pthread_mutex_unlock(&(db->mutex_compaction));

        Table* table = db->active_table[1];

        MTable* mt = table_to_metatable(table);

        mt->mtid = db->mtid_next;

        vc_insert(db->vcroot, mt);

        uint64_t i = 0;
        meta_table_dump(db->store_dir, i, mt);

        db_log(db, "active table dumper");

        db->mtid_next++;

        if(db->vcroot->cc.count >= 4)
        {
            pthread_mutex_lock(&(db->mutex_compaction));
            pthread_cond_signal(&(db->cond_consumer));
            pthread_mutex_unlock(&(db->mutex_compaction));
        }

        db->active_table[1] = NULL;
    }

    pthread_exit(0);
}

void db_wait_active_table(DB* db)
{
    pthread_mutex_lock(&(db->mutex_active));
    while(table_full(db->active_table[0]))
    {
        pthread_cond_signal(&(db->cond_active));
        pthread_cond_wait(&(db->cond_writer), &(db->mutex_active));
    }
    pthread_mutex_unlock(&(db->mutex_active));
}

void create_threads(DB* db)
{
    int ar = pthread_create(&(db->active_id), NULL, active_dumper, (void *)db);
    pthread_setname_np(db->active_id, "Active-Dumper");

    if(ar != 0)
        printf("fail to create the thread\n");

    int cr = pthread_create(&(db->compaction_id), NULL, thread_compaction, (void *)db);
    pthread_setname_np(db->compaction_id, "Compaction   ");

     if(cr != 0)
        printf("fail to create the thread\n");
}

static void db_init(DB* db)
{
    db->store_dir = "store_dir";
 
    db->active_table[0] = table_malloc();
    db->active_table[1] = NULL;

    pthread_mutex_init(&(db->mutex_active), NULL);
    pthread_mutex_init(&(db->mutex_compaction), NULL);

    pthread_cond_init(&(db->cond_active), NULL);
    pthread_cond_init(&(db->cond_writer), NULL);
    pthread_cond_init(&(db->cond_producer), NULL);
    pthread_cond_init(&(db->cond_consumer), NULL);

    db->mtid_next = 1;
    db->vcid_next = 1;

    db->vcroot = vc_create(0);
    db->vcroot->vc_id = 0;

    FILE * const log = fopen("log.txt", "w");
    db->log = log;
}

static bool db_bulid_dir(char* root_dir, char* sub_dir)
{
  char path[256];
  sprintf(path, "%s/%s", root_dir, sub_dir);
  struct stat stat_buf;
  mkdir(path, 00755);
  if (0 != access(path, F_OK)) { return false; }
  if (0 != stat(path, &stat_buf)) { return false; }
  if (!S_ISDIR(stat_buf.st_mode)) {return false; }
  return true;
}

DB* db_create()
{
    DB* db = (DB *)malloc(sizeof(DB));

    char path[4096];
    sprintf(path, "%s", "store_dir");
    if (false == db_bulid_dir(path, "")) return NULL;

    char sub_dir[32];
    for (uint64_t i = 0; i < 32; i++) 
    {
        sprintf(sub_dir, "%02" PRIx64, i);
        if (false == db_bulid_dir(path, sub_dir)) return NULL;
    }

    db_init(db);

    create_threads(db);

    return db;
}

void db_close(DB* db)
{
    pthread_mutex_destroy(&(db->mutex_active));
    pthread_mutex_destroy(&(db->mutex_compaction));

    pthread_cond_destroy(&(db->cond_active));
    pthread_cond_destroy(&(db->cond_writer));
    pthread_cond_destroy(&(db->cond_producer));
    pthread_cond_destroy(&(db->cond_consumer));

    fclose(db->log);
    free(db);
}

static bool db_insert_try(DB* db, char* key, char* value)
{
    Table* at = db->active_table[0];
    bool r = table_insert_kv(at, key, value);

    return r;
}

bool Put(DB* db, char* key, char* value)
{
    if(db_insert_try(db, key, value) == false)
        db_wait_active_table(db);

    return true;
}

static bool recursive_lookup(VirtualContainer* vr, char* key, uint8_t* hash)
{
    bool search = false;

    for(int i=0; i<vr->cc.count; i++)
    {
        MTable *mt = vr->cc.mtables[i];
        search = metatable_lookup(mt, key, hash);

        if(search)
            return true;
    }

    int vc_id = compaction_select(hash, vr->start_bit + 3);

    if(vr->sub_vc[vc_id])
        search = recursive_lookup(vr->sub_vc[vc_id], key, hash);

    return search;
}

void Get(DB* db, char* key)
{
    uint8_t hash[20];
    bool search = false;

    SHA1((uint8_t *)key, sizeof(key), hash);

    for(int i=0; i<2; i++)
    {
        Table* t = db->active_table[i];

        if(t)
            search = table_lookup(t, key, hash);

        if(search)
            break;
    }

    if(!search)
        search = recursive_lookup(db->vcroot, key, hash);

    if(!search)
        printf("search fail!\n");
}