#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <regex.h>
#include <sys/time.h>
#include <unistd.h> 
#include "args.h"

uint64_t micros_since_epoch(){
    struct timeval tv;
    uint64_t micros = 0;
    gettimeofday(&tv, NULL);  
    micros =  ((uint64_t)tv.tv_sec) * 1000000 + tv.tv_usec;
    return micros;
}

struct arguments arguments;

typedef struct __dimension{
    char name[16];
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
array_schema_t* parse_array_schema(char *array_schema);
array_schema_t* exemple_array_schema(char *array_schema);
void create_array(array_schema_t* array_schema);
void create_array(array_schema_t* array_schema);
void create_chunk(  array_schema_t* array_schema, 
                    char *file_path, 
                    uint64_t* chunk_idx, 
                    char *buf);
size_t get_type_size(uint16_t attr_type);



int main(int argc, char *argv[]) {

    int dim;
    /* Default values. */
    arguments.array_schema = "";
    arguments.destination = "./";
    arguments.verbose = 0;
    arguments.help = 0;

    argp_parse (&argp, argc, argv, 0, 0, &arguments);

    array_schema_t *array_schema = parse_array_schema(arguments.array_schema);
    if(arguments.verbose){
        print_array_schema(array_schema);
    }
    uint64_t start = micros_since_epoch();
    create_array(array_schema);
    uint64_t end = micros_since_epoch();
    double dif = (double) end-start;
    printf("Total Time: %f us\n",dif);
    
    uint64_t n_ele=1;
    for(dim=0; dim<array_schema->dim_len;++dim){
        n_ele*=(array_schema->dimensions[dim]->end - array_schema->dimensions[dim]->start +1);
    }
    double total_writed = (double)n_ele*get_type_size(array_schema->attr_type);
    printf("Writed: %.0lf B\n",total_writed);
    printf("Speed: %lf MB/s\n", (total_writed/(1024.*1024))/(dif/1000000.));

//    free_array_schema(array_schema);

    return 0;
}
void print_array_schema(array_schema_t* array_schema){
    int i;
    printf("<");
    switch(array_schema->attr_type){
        case 1: printf("uint32_t"); break;
        case 2: printf("uint64_t"); break;
        case 3: printf("float"); break;
        case 4: printf("double"); break;
        default:
            printf("unknown");
    }
    printf(">[");
    for(i=0; i<array_schema->dim_len; ++i){
        if(i>0){
            printf(";");
        }
        printf("%s:%ld:%ld:%ld:%ld", 
                array_schema->dimensions[i]->name,
                array_schema->dimensions[i]->start,
                array_schema->dimensions[i]->end,
                array_schema->dimensions[i]->overlap,
                array_schema->dimensions[i]->chunk_length);
    }
    printf("]\n");
    printf("dim: (delta,n_chunks)\n");
    for(i=0; i<array_schema->dim_len; ++i){
    printf("%s: (%ld,%ld)\n", 
            array_schema->dimensions[i]->name,
            array_schema->dimensions[i]->delta, 
            array_schema->dimensions[i]->n_chunks);
    }
    printf("total chunk len: %ld\n", array_schema->chunk_length);
}




/* Parses array schema string */
array_schema_t* exemple_array_schema(char *array_schema){

    int i;
    array_schema_t* schema = (array_schema_t*)malloc(sizeof(array_schema_t));
    schema->attr_type = 1;
    schema->dimensions = (dimenstion_t**)malloc(sizeof(dimenstion_t*)*3);
    
    dimenstion_t* col = (dimenstion_t*)malloc(sizeof(dimenstion_t));
    sprintf(col->name,"col");
    col->start = 1;
    col->end = 50000;
    col->overlap = 0;
    col->chunk_length = 2000;
    col->delta = 1;
    col->n_chunks = (col->end - col->start + 1)/col->chunk_length;
    
    dimenstion_t* row = (dimenstion_t*)malloc(sizeof(dimenstion_t));
    sprintf(row->name,"row");
    row->start = 1;
    row->end = 50000;
    row->overlap = 0;
    row->chunk_length = 2000;
    row->delta = col->delta*(col->end - col->start + 1);
    row->n_chunks = (row->end - row->start + 1)/row->chunk_length;

    dimenstion_t* time = (dimenstion_t*)malloc(sizeof(dimenstion_t));
    sprintf(time->name,"time");
    time->start = 1;
    time->end = 10;
    time->overlap = 0;
    time->chunk_length = 2;
    time->delta = row->delta*(row->end - row->start + 1);
    time->n_chunks = (time->end - time->start + 1)/time->chunk_length;
    
    dimenstion_t* band = (dimenstion_t*)malloc(sizeof(dimenstion_t));
    sprintf(band->name,"band");
    band->start = 1;
    band->end = 3;
    band->overlap = 0;
    band->chunk_length = 1;
    band->delta = time->delta *(time->end - time->start + 1);
    band->n_chunks = (band->end - band->start + 1)/band->chunk_length;

    schema->dim_len = 3;
    schema->dimensions[0] = col;
    schema->dimensions[1] = row;
    schema->dimensions[2] = time;
    schema->dimensions[3] = band;
    
    schema->chunk_length = 1;
    for(i=0; i<schema->dim_len; ++i){
        schema->chunk_length*=schema->dimensions[i]->chunk_length;
    }
    
    return schema;
}


void create_array(array_schema_t* array_schema){
    int c, dim;
    uint64_t *chunk_idx;
    char *buf  = (char*)malloc(get_type_size(array_schema->attr_type)*array_schema->chunk_length);
    uint64_t total_chunks = 1;
    chunk_idx = (uint64_t *)malloc(sizeof(uint64_t)*array_schema->dim_len);
    for(dim=0; dim<array_schema->dim_len; ++dim){
        total_chunks*= array_schema->dimensions[dim]->n_chunks;
    }
    
    char file_path[128];

    if(arguments.verbose) {
        printf("Starting array creation ...\n");
    }
    for(c=0; c<total_chunks; ++c){
        // calcula chunk
        sprintf(file_path, "%s/%d.chunk",arguments.destination,c);
        create_chunk(array_schema, file_path, chunk_idx, buf);
        // incrementa cada dimensao
        for(dim=0; dim<array_schema->dim_len; ++dim){
            chunk_idx[dim]+=array_schema->dimensions[dim]->chunk_length;
            if(chunk_idx[dim] >= array_schema->dimensions[dim]->end){
                chunk_idx[dim] = 0;
            }else{
                break;
            }
        }
    }
    if(arguments.verbose) {
        printf("Array creation finished.\n");
    }
    free(chunk_idx);
    free(buf);
}


void create_chunk(  array_schema_t* array_schema, 
                    char *file_path, 
                    uint64_t* chunk_idx, 
                    char *buf){
    int dim;
    uint64_t start_value;
    uint64_t chunk_id =0;
    uint64_t acc_n_chunks = 0;
    uint64_t i;
    FILE *f;
    if(arguments.verbose) {
        printf("Creating chunk:\n");
        printf("\tpath: %s\n", file_path);
        printf("\tindexes: ");
        for(dim=0; dim<array_schema->dim_len; ++dim){
            printf("%ld,", chunk_idx[dim]
                            /array_schema->dimensions[dim]->chunk_length);
        }
        printf("\n");
    }
    acc_n_chunks =1;
    for(dim=0; dim<array_schema->dim_len; ++dim){
        chunk_id+= (chunk_idx[dim]/array_schema->dimensions[dim]->chunk_length)
                    * acc_n_chunks;
        acc_n_chunks*=array_schema->dimensions[dim]->n_chunks;
    }
    start_value = chunk_id*array_schema->chunk_length;
    if(arguments.verbose) {
        printf("\tchunk_id: %ld, start_value: %ld\n", chunk_id, start_value);
    }
    
    f = fopen(file_path, "w");
    if (f == NULL) {
        fprintf(stderr, "Error opening file '%s' for writing.\n", file_path);
        return;
    }
    uint32_t *uint32_buf;
    uint64_t *uint64_buf;
    float *float_buf;
    double *double_buf;
    
    switch(array_schema->attr_type){
        case 1: 
            uint32_buf = (uint32_t*) buf;
            for(i=0; i<array_schema->chunk_length; ++i){
                uint32_buf[i] = (uint32_t)start_value+i;
            }
            break;
        case 2: 
            uint64_buf = (uint64_t*) buf;
            for(i=0; i<array_schema->chunk_length; ++i){
                uint64_buf[i] = (uint64_t)start_value+i;
            }
            break;
        case 3: 
            float_buf = (float*) buf;
            for(i=0; i<array_schema->chunk_length; ++i){
                float_buf[i] = (float)start_value+i;
            }
            break;
        case 4: 
            double_buf = (double*) buf;
            for(i=0; i<array_schema->chunk_length; ++i){
                double_buf[i] = (double)start_value+i;
            }
            break;
    }
    fwrite(buf, sizeof(char), 
                    array_schema->chunk_length 
                    * get_type_size(array_schema->attr_type), 
                    f);
    fclose(f);
}


size_t get_type_size(uint16_t attr_type){
    switch(attr_type){
    case 1: return sizeof(uint32_t);  
    case 2: return sizeof(uint64_t);
    case 3: return sizeof(float);
    case 4: return sizeof(double);
    }
    return sizeof(uint32_t);
}



void free_array_schema(array_schema_t* array_schema){
    int i;
    for(i=0; i<array_schema->dim_len; ++i){
        printf("free %d\n", i);
        free(array_schema->dimensions[i]);
    }
    free(array_schema);
}



array_schema_t* parse_array_schema(char *array_schema){
    // TODO implementar parser
    //    <double>[col=1:1000:0:100,row=1:1000:0:100,time=1:100,0:10]
    return exemple_array_schema(array_schema);
    
    /*
    regex_t regex;
    int reti;
    // "<double>[col=1:1000:0:100;row=1:1000:0:100;time=1:100:0:10]" -d /tmp/chunkier
    reti = regcomp(&regex, "<([a-z0-9]+)>\\[([a-z0-9=:;]+)\\]", REG_EXTENDED);
    if (reti) {
        fprintf(stderr, "Could not compile regex\n");
        exit(1);
    }
    
    
    size_t maxMatches = 2;
    size_t maxGroups = 3;
    unsigned int m = 0;
    char *cursor;
    
    char cursorCopy[100];
    regmatch_t groupArray[3];
    cursor = array_schema;
    int type_ok = 0;
    int dim_ok = 0;
    for (m = 0; m < maxMatches; m ++){
        if (regexec(&regex, cursor, maxGroups, groupArray, 0)){
            break;  // No more matches
        }

        unsigned int g = 0;
        unsigned int offset = 0;
        for (g = 0; g < maxGroups; g++){
            if (groupArray[g].rm_so == (size_t)-1){
                break;  // No more groups
            }

            if (g == 0){
                offset = groupArray[g].rm_eo;
            }
        
          strcpy(cursorCopy, cursor);
          cursorCopy[groupArray[g].rm_eo] = 0;
          printf("Match %u, Group %u: [%2u-%2u]: %s\n",
                 m, g, groupArray[g].rm_so, groupArray[g].rm_eo,
                 cursorCopy + groupArray[g].rm_so);
        }
        cursor += offset;
    }

    //regfree(&regex);
    //printf("pronto\n");
    */
}
