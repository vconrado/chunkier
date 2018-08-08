#ifndef __CHUNKIER_ARGS_H__
#define __CHUNKIER_ARGS_H__

#include <argp.h>

const char *argp_program_version = "chunkier 0.1";
const char *argp_program_bug_address = "<vconrado@gmail.com>";
static char doc[] = "Chunkier creates chunks with arbitrary dimensions and sizes.";
//static char args_doc[] = " \"<double>[col=1:1000:0:100,row=1:1000:0:100,time=1:100,0:10]\" ...";
static char args_doc[] = " \"uint32_t|uint64_t|float|double col_start col_end col_chunk row_start row_end row_chunk time_start time_end time_chunk\" ...";
static struct argp_option options[] = { 
    {"dest",            'd',    "FOLDER",  0,  "Folder destination"},
    {"verbose",         'v',    0,              0,  "Produce verbose output" },
    { 0 } 
};

struct arguments
{
  char *array_schema;
  char *destination;
  int verbose;
  int help;
  char *args[10];
};

static error_t
parse_opt (int key, char *arg, struct argp_state *state) {
  /* Get the input argument from argp_parse, which we
     know is a pointer to our arguments structure. */
    struct arguments *arguments = state->input;

    switch (key){
        case 'v':
            arguments->verbose = 1;
            break;
        case 'd':
            arguments->destination = (char*)malloc(sizeof(char)*strlen(arg));
            strcpy(arguments->destination,arg);
            break;
        case ARGP_KEY_ARG:
            if (state->arg_num >= 10){
                /* Too many arguments. */
                argp_usage (state);
            }
            arguments->args[state->arg_num] = (char*)malloc(sizeof(char)*strlen(arg));
            strcpy(arguments->args[state->arg_num],arg);
          break;
        case ARGP_KEY_END:
            if (state->arg_num < 10){
                /* Not enough arguments. */
                argp_usage (state);
            }
          break;
        default:
            return ARGP_ERR_UNKNOWN;
    }
  return 0;
}

static struct argp argp = { options, parse_opt, args_doc, doc };


#endif /*__CHUNKIER_ARGS_H__*/
