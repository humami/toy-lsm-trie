#include "bloom_fliter.h"
#include <string.h>

int seed[3] = {13, 107, 213};

static int _hash(char* key, int seed)
{
    int hv = 0;

    int len = strlen(key);

    for(int i=0; i<len; i++)
        hv += seed * hv + (int)key[i]; 

    return hv % 8000;
}

static void set_bit(BloomFliter *bf, int index)
{
    bf->bit[index / 8] |= (1 << (index % 8));
}

static bool get_bit(BloomFliter *bf, int index)
{
    return bf->bit[index / 8] & (1 << (index % 8));
}

void bf_add(BloomFliter *bf, char* key)
{
    for(int i=0; i<3; i++)
    {
        int index = _hash(key, seed[i]);

        set_bit(bf, index);
    }
}

bool bf_match(BloomFliter *bf, char* key)
{
    bool find = true;

    for(int i=0; i<3; i++)
    {
        int index = _hash(key, seed[i]);

        find  = find && get_bit(bf, index);
    }

    return find;
}