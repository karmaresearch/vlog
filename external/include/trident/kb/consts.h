/*
   Copyright (C) 2015 Jacopo Urbani.

   This file is part of Trident.

   Trident is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 2 of the License, or
   (at your option) any later version.

   Trident is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Trident.  If not, see <http://www.gnu.org/licenses/>.
*/

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

#define ROW_LAYOUT 0
#define CLUSTER_LAYOUT 1
#define COLUMN_LAYOUT 2

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

#endif /* CONSTS_H_ */
