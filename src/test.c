//  && ./match

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <regex.h>
#include <stdint.h>


#define MAX_ARRAY_DIM 10
#define MAX_DIM_NAME 16
typedef struct __dimension{
    char name[(MAX_DIM_NAME+1)];
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

array_schema_t * trata_dims(char *source){
    char * regex_dims = "^([a-z]+)=([0-9]+):([0-9]+):([0-9]+):([0-9]+)";
    char *data;
    regex_t regex_dims_compiled;
    array_schema_t* schema = (array_schema_t*)malloc(sizeof(array_schema_t));
    schema->attr_type = 0;
    schema->dimensions = (dimenstion_t**)malloc(sizeof(dimenstion_t*)*MAX_ARRAY_DIM);
    schema->dim_len=0;
    
    int m;
    int i;
    size_t maxMatches = MAX_ARRAY_DIM;
    size_t maxGroups = 10;
    char *cursor_dims;
    regmatch_t groupArray[maxGroups];
    
    if (regcomp(&regex_dims_compiled, regex_dims, REG_EXTENDED)){
        printf("Could not compile regex_dims expression.\n");
        exit(1);
    };
    
    cursor_dims = source;
    for (m = 0; m < maxMatches; m ++){
        if (regexec(&regex_dims_compiled, cursor_dims, maxGroups, groupArray, 0)){
            break;  // No more matches
        }

        unsigned int g = 0;
        unsigned int offset = 0;
        dimenstion_t* dim = NULL;
        for (g = 0; g < maxGroups; g++) {
            if (groupArray[g].rm_so == (size_t)-1){
                printf("aqui\n");
                break;  // No more groups
            }
            
            char cursor_baseCopy[strlen(cursor_dims) + 1];
            strcpy(cursor_baseCopy, cursor_dims);
            cursor_baseCopy[groupArray[g].rm_eo] = 0;
            data = cursor_baseCopy + groupArray[g].rm_so;
            switch(g){
                case 0:
                    offset = groupArray[g].rm_eo;
                    schema->dim_len=schema->dim_len+1;
                     dim = (dimenstion_t*)malloc(sizeof(dimenstion_t));
                     dim->delta = 1;
                    break;
                case 1: // dim name
                    if(strlen(data) > MAX_DIM_NAME){
                        printf("Dimension '%s' truncated to ", data);
                        data[MAX_DIM_NAME+1]=0;
                        printf("'%s',\n", data);
                    }
                    strcpy(dim->name, data);
                    break;
                case 2: // dim start
                    dim->start = atol(data);
                    break;
                case 3: // dim end
                    dim->end = atol(data);
                    break;
                case 4: // overlap
                    dim->overlap = atol(data);
                    break;
                case 5: // chunk_length
                    dim->chunk_length = atol(data);
                    break;
            }
            dim->n_chunks = (dim->end - dim->start + 1)/dim->chunk_length;
            schema->dimensions[m] = dim;
            printf("Match %u, Group %u: [%2u-%2u]: %s\n",
                    m, g, groupArray[g].rm_so, groupArray[g].rm_eo,
                    cursor_baseCopy + groupArray[g].rm_so);
        }
        cursor_dims += offset;
    }
    schema->chunk_length = 1;
    for(i=0; i<schema->dim_len; ++i){
        schema->chunk_length*=schema->dimensions[i]->chunk_length;
    }
    regfree(&regex_dims_compiled);
    
    return schema;
}


int main (){
    char * source = "<float>[col=:1000:0:10;row=1:1000:0:10;time=1:10:0:2]";
    char * regex_base = "^<([a-z]+)>\\[([a-z0-9:;=]+)\\]$";
    size_t maxMatches = 2;
    size_t maxGroups = 3;
    array_schema_t *array_schema;

    regex_t regex_base_compiled;

    regmatch_t groupArray[maxGroups];
    unsigned int m;
    char *cursor_base;
    char *cursor_type;
    char *cursor_dims;

    if (regcomp(&regex_base_compiled, regex_base, REG_EXTENDED)){
        printf("Could not compile regex_base expression.\n");
        return 1;
    };

    m = 0;
    cursor_base = source;
    uint16_t type = 0;
    for (m = 0; m < maxMatches; m ++){
        if (regexec(&regex_base_compiled, cursor_base, maxGroups, groupArray, 0)){
            break;  // No more matches
        }

        unsigned int g = 0;
        unsigned int offset = 0;
        for (g = 0; g < maxGroups; g++) {
            if (groupArray[g].rm_so == (size_t)-1){
                break;  // No more groups
            }
            char cursor_baseCopy[strlen(cursor_base) + 1];
                strcpy(cursor_baseCopy, cursor_base);
                cursor_baseCopy[groupArray[g].rm_eo] = 0;
            switch(g){
                case 0: 
                    offset = groupArray[g].rm_eo;
                    break;
                case 1:
                    cursor_type = cursor_baseCopy + groupArray[g].rm_so;
                    if (strcmp(cursor_type, "uint32_t") == 0){
                        type= 1;
                    }else if (strcmp(cursor_type, "uint64_t") == 0) {
                        type= 2;
                    }else if (strcmp(cursor_type, "float") == 0) {
                        type= 3;
                    }else if (strcmp(cursor_type, "uint64_t") == 0) {
                        type= 4;
                    }else {
                        fprintf(stderr,"Invalid type '%s'\n", cursor_type);
                        exit(1);
                    }
                    break;
                case 2:
                    cursor_dims = cursor_baseCopy + groupArray[g].rm_so;
                    printf("tratar dims\n");
                    array_schema = trata_dims(cursor_dims);
                    break;
                default:
                    printf("Invalid groups length\n");
            }
        }
        cursor_base += offset;
    }
    regfree(&regex_base_compiled);
    
    if(array_schema != NULL){
        array_schema->attr_type = type;
        print_array_schema(array_schema);
    }else{
        printf("NULL array_schema\n");
    }
    return 0;
}
