#include "array_schema.h"

void DEBUG(int line){
    printf("DEBUG %d\n", line);
    fflush(stdout);
}

array_schema_t * parse_dims(char *source){
    char * regex_dims = "^([a-z]+)=([0-9]+):([0-9]+):([0-9]+):([0-9]+)[;]?";
    char *data;
    regex_t regex_dims_compiled;
    array_schema_t* schema = (array_schema_t*)malloc(sizeof(array_schema_t));
    schema->attr_type = 0;
    schema->dimensions = (dimenstion_t**)malloc(sizeof(dimenstion_t*)*MAX_ARRAY_DIM);
    schema->dim_len=0;
    
    int m;
    int i;
    size_t maxMatches = MAX_ARRAY_DIM;
    size_t maxGroups = (MAX_ARRAY_DIM);
    char *cursor_dims;
    regmatch_t groupArray[MAX_ARRAY_DIM];
    
    if (regcomp(&regex_dims_compiled, regex_dims, REG_EXTENDED)){
        printf("Could not compile regex_dims expression.\n");
        exit(1);
    };
    
    cursor_dims = source;
    for (m = 0; m < maxMatches; m ++){
        int reti = regexec(&regex_dims_compiled, cursor_dims, maxGroups, groupArray, 0);
        if (reti){
            if(strlen(cursor_dims) != 0){
                printf("Error parsing dimension scheme: '%s'.\n", cursor_dims);
                exit(1);
            }else{
                // end of string
                break;
            }
        }

        unsigned int g = 0;
        unsigned int offset = 0;
        dimenstion_t* dim = NULL;
        for (g = 0; g < maxGroups; g++) {
            if (groupArray[g].rm_so == (size_t)-1){
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
                    strncpy(dim->name, data, MAX_DIM_NAME);
                    if(strlen(data) > MAX_DIM_NAME){
                        dim->name[MAX_DIM_NAME] = 0;
                        printf("Dimension '%s' truncated to '%s',\n", data, dim->name);
                    }

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
            printf("Match %u, Group %u: [%2u-%2u]: %s\n",
                    m, g, groupArray[g].rm_so, groupArray[g].rm_eo,
                    cursor_baseCopy + groupArray[g].rm_so); 
            
        }
        dim->n_chunks = (dim->end - dim->start + 1)/dim->chunk_length;
            schema->dimensions[m] = dim;
        cursor_dims += offset;
    }
    schema->chunk_length = 1;
    for(i=0; i<schema->dim_len; ++i){
        schema->chunk_length*=schema->dimensions[i]->chunk_length;
    }
    regfree(&regex_dims_compiled);
    
    return schema;
}


array_schema_t* parse_array_schema(char *source){
    char * regex_base = "^<([a-z]+)>\\[([a-z0-9:;=]+)\\]$";
    size_t maxMatches = 2;
    size_t maxGroups = 3;
    array_schema_t *array_schema = NULL;

    regex_t regex_base_compiled;

    regmatch_t groupArray[maxGroups];
    unsigned int m;
    char *cursor_base;
    char *cursor_type;
    char *cursor_dims;

    if (regcomp(&regex_base_compiled, regex_base, REG_EXTENDED)){
        printf("Could not compile regex_base expression.\n");
        exit(1);
    }
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
                    }else if (strcmp(cursor_type, "double") == 0) {
                        type= 4;
                    }else {
                        fprintf(stderr,"Invalid type '%s'\n", cursor_type);
                        exit(1);
                    }
                    break;
                case 2:
                    cursor_dims = cursor_baseCopy + groupArray[g].rm_so;
                    array_schema = parse_dims(cursor_dims);
                    if(array_schema != NULL){
                        array_schema->attr_type = type;
                    }else{
                        fprintf(stderr,"Invalid schema\n");
                        exit(1);
                    }
                    break;
                default:
                    printf("Invalid group length\n");
            }
        }
        cursor_base += offset;
    }
    regfree(&regex_base_compiled);
    
    /*
    if(array_schema != NULL){
        array_schema->attr_type = type;
        print_array_schema(array_schema);
    }else{
        printf("NULL array_schema\n");
    }*/
    return array_schema;
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

void free_array_schema(array_schema_t* array_schema){
    int i;
    for(i=0; i<array_schema->dim_len; ++i){
        printf("free %d\n", i);
        free(array_schema->dimensions[i]);
    }
    free(array_schema);
}
