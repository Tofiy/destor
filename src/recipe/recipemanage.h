/*
 * recipemanage.h
 *
 *  Created on: May 22, 2012
 *      Author: fumin
 */

#ifndef RECIPEMANAGE_H_
#define RECIPEMANAGE_H_

#include "../destor.h"

/* a backup version */
struct backupVersion {

	sds path;
	int32_t number; /* backup version numer start from 0 */

	int deleted;

	int64_t number_of_files;
	int64_t number_of_chunks;

	sds fname_prefix; /* The prefix of the file names */

	FILE *metadata_fp;
	FILE *recipe_fp;
	FILE *seed_fp;
};

struct chunkPointer {
	fingerprint fp;
	containerid id;
};

struct recipe {
	int64_t chunknum;
	int64_t filesize;
	sds filename;
};

void init_recipe_management();
void close_recipe_management();

struct backupVersion* create_backup_verion(const char *path);
int backup_version_exists(int number);
struct backupVersion* open_backup_version(int number);
void update_backup_version(struct backupVersion *b);
void free_backup_version(struct backupVersion *b);

void append_recipe_meta(struct backupVersion* b, struct recipe* r);
void append_n_chunk_pointers(struct backupVersion* b, struct chunkPointer* cp,
		int n);
struct recipe* read_next_recipe_meta(struct backupVersion* b);
struct recipe* read_next_n_chunk_pointers(struct backupVersion* b, int n,
		struct chunkpointer** cp, int *k);
void append_seed(struct backupVersion* b, containerid id, int32_t size);
struct recipe* new_recipe(char* name);
void free_recipe(struct recipe* r);

#endif /* BVMANAGE_H_ */