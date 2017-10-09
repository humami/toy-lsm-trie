#define _GNU_SOURCE

#include "db.h"
#include "table.h"

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

void *test_thread(void *arg)
{    
    DB* db = (DB *)arg;

    while(true)
    {
        char k[160];
        char key[160];
        char value[1024];

        int i = 0;
        int j;

        for(i=0; i<5000; i++)
        {
            sprintf(key, "%d", i);
            sprintf(value, "%d", i*2);

            Put(db, key, value);

            bzero(key, sizeof(key));
        }

        printf("insert ok!!!!!!\n\n");
        
        while(true)
        {
            int i;
            printf("please input the key which you want to search\n");
            scanf("%d", &i);
            
            sprintf(key, "%d", i);

            Get(db, key);
            
            bzero(key, sizeof(key));
        }
    }

    pthread_exit(0);
}

void db_test()
{
    DB* db = NULL;

    db = db_create();

    pthread_t pid;

    int r = pthread_create(&pid, NULL, test_thread, (void *)db);

    if(r != 0)
        printf("fail to create the thread\n");

    pthread_join(pid, NULL);
    pthread_join(db->active_id, NULL);
    pthread_join(db->compaction_id, NULL);

    db_close(db);
}


int main()
{
    printf("*********Test Unit*********\n");

    db_test();

    return 0;
}