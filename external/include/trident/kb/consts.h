#ifndef CONSTS_H_
#define CONSTS_H_

#include <inttypes.h>

//This is a list of all permutation IDs. It is replicated in tridentcompr/Compressor.h
#define IDX_SPO 0
#define IDX_OPS 1
#define IDX_POS 2
#define IDX_SOP 3
#define IDX_OSP 4
#define IDX_PSO 5

#define ROW_ITR 0
#define CLUSTER_ITR 1
#define COLUMN_ITR 2
#define NEWCOLUMN_ITR 3
#define ARRAY_ITR 4
#define CACHE_ITR 5
#define AGGR_ITR 6
#define SCAN_ITR 7
#define SIMPLESCAN_ITR 8
#define EMPTY_ITR 9
#define TERM_ITR 10

//Use for dynamic layout
#define DIFFERENCE 0
#define NO_DIFFERENCE 1
#define COMPR_1 0
#define COMPR_2 1
#define NO_COMPR 2

//Size buffer (i.e. number of elements) to sort during the compression
#define SORTING_BLOCK_SIZE 25000000

#define MAX_N_BLOCKS_IN_CACHE 1000000

//Used in the dictionary lookup thread
#define OUTPUT_BUFFER_SIZE 2048
#define MAX_N_PATTERNS 10

//StringBuffer block size
#define SB_BLOCK_SIZE 65536

//Generic options
#define N_PARTITIONS 6
#define THRESHOLD_KEEP_MEMORY 100*1024

#define MAX_N_FILES 2048

//Used in the cache of the tree to serialize the nodes
#define SIZE_SUPPORT_BUFFER 512 * 1024

#define MAX_SESSIONS 1024
#define NO_BLOCK_SESSION -1
#define EMPTY_SESSION -2
#define FREE_SESSION -3

//Size indices in the binary tables
#define ADDITIONAL_SECOND_INDEX_SIZE 512
#define FIRST_INDEX_SIZE 256

#endif /* CONSTS_H_ */
