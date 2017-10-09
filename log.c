#define _GNU_SOURCE

#include <unistd.h>
#include <string.h>
#include "log.h"

void db_log(DB *db, char *msg)
{
    if(!db->log)
        return;
    
    char thread_name[32] = {0};
    pthread_getname_np(pthread_self(), thread_name, sizeof(thread_name));

    db->log = fopen("log.txt", "a");

    fprintf(db->log, "%s          ", thread_name);
    fprintf(db->log, "%s\n", msg);

    fclose(db->log);
}