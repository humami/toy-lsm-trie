#pragma once

#include <stdio.h>
#include <inttypes.h>
#include <stdbool.h>

typedef struct BloomFliter
{
	char bit[1000];
}BloomFliter;

void bf_add(BloomFliter *bf, char* key);

bool bf_match(BloomFliter *bf, char* key);
