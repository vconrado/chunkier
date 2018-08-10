#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <regex.h>
#include <sys/time.h>
#include <unistd.h> 
#include "args.h"
#include "array_schema.h"

uint64_t micros_since_epoch(){
    struct timeval tv;
    uint64_t micros = 0;
    gettimeofday(&tv, NULL);  
    micros =  ((uint64_t)tv.tv_sec) * 1000000 + tv.tv_usec;
    return micros;
}

struct arguments arguments;


//array_schema_t* parse_array_schema(char *array_schema);
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
    arguments.trial = 0;
    arguments.verbose = 0;
    arguments.help = 0;

    argp_parse (&argp, argc, argv, 0, 0, &arguments);

  array_schema_t *array_schema = parse_array_schema(arguments.array_schema);
//    array_schema_t *array_schema = exemple_array_schema(arguments.array_schema);
    

    if(arguments.verbose){
        print_array_schema(array_schema);
    }
    uint64_t start = micros_since_epoch();
    create_array(array_schema);
    uint64_t end = micros_since_epoch();
    double dif = (double) end-start;

    if(arguments.trial == 0){
        printf("Total Time: %f us\n",dif);
        
        uint64_t n_ele=1;
        for(dim=0; dim<array_schema->dim_len;++dim){
            n_ele*=(array_schema->dimensions[dim]->end - array_schema->dimensions[dim]->start +1);
        }
        double total_writed = (double)n_ele*get_type_size(array_schema->attr_type);
        printf("Writed: %.0lf B\n",total_writed);
        printf("Speed: %lf MB/s\n", (total_writed/(1024.*1024))/(dif/1000000.));
    } else {
        printf("Trial mode. No chunks created.\n");
    }
//    free_array_schema(array_schema);

    return 0;
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
    col->end = 8;
    col->overlap = 0;
    col->chunk_length = 2;
    col->delta = 1;
    col->n_chunks = (col->end - col->start + 1)/col->chunk_length;
    
    dimenstion_t* row = (dimenstion_t*)malloc(sizeof(dimenstion_t));
    sprintf(row->name,"row");
    row->start = 1;
    row->end = 8;
    row->overlap = 0;
    row->chunk_length = 2;
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

    schema->dim_len = 2;
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
    FILE *f = NULL;
    /*if(arguments.verbose) {
        printf("Creating chunk:\n");
        printf("\tpath: %s\n", file_path);
        printf("\tindexes: ");
        for(dim=0; dim<array_schema->dim_len; ++dim){
            printf("%ld,", chunk_idx[dim]
                            /array_schema->dimensions[dim]->chunk_length);
        }
        printf("\n");
    }*/
    acc_n_chunks =1;
    for(dim=0; dim<array_schema->dim_len; ++dim){
        chunk_id+= (chunk_idx[dim]/array_schema->dimensions[dim]->chunk_length)
                    * acc_n_chunks;
        acc_n_chunks*=array_schema->dimensions[dim]->n_chunks;
    }
    start_value = chunk_id*array_schema->chunk_length;
    
    if(arguments.trial == 0) {
        f = fopen(file_path, "w");
        if (f == NULL) {
            fprintf(stderr, "Error opening file '%s' for writing.\n", file_path);
            return;
        }
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
    if(arguments.trial == 0) {
        fwrite(buf, sizeof(char), 
                        array_schema->chunk_length 
                        * get_type_size(array_schema->attr_type), 
                        f);
        fclose(f);
    }
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
