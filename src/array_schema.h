#ifndef __CHUNKIER_PARSER_H__
#define __CHUNKIER_PARSER_H__

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <regex.h>
#include <stdint.h>

#define MAX_ARRAY_DIM 10
#define MAX_DIM_NAME 16

typedef struct __dimension{
    char name[MAX_DIM_NAME+1];
    uint64_t start;
    uint64_t end;
    uint64_t overlap;
    uint64_t chunk_length;
    uint64_t delta; // distancia entre cada elemento nessa dimensao
    uint64_t n_chunks; // numero de chunks nessa dimensao
} dimenstion_t;

typedef struct __array_schema{
    uint64_t dim_len;
    dimenstion_t **dimensions;
    uint16_t attr_type; // 1: uint32_t, 2: uint64_t, 3: float, 4: double
    uint64_t chunk_length;
} array_schema_t;

void print_array_schema(array_schema_t* array_schema);
void free_array_schema(array_schema_t* array_schema);
array_schema_t * parse_dims(char *source);
array_schema_t* parse_array_schema(char *source);

#endif /* __CHUNKIER_PARSER_H__ */
