/*
 * near_exact_locality_index.c
 *
 *  Created on: Nov 15, 2013
 *      Author: fumin
 */

#include "near_exact_locality_index.h"
#include "feature_index.h"
#include "../tools/lru_cache.h"

static struct lruCache* container_meta_cache = NULL;
static containerid cid = TEMPORARY_ID;

void init_near_exact_locality_index() {

	init_feature_index();

	if (destor.index_feature_method[1] > 1) {
		/*
		 * If destor.index_feature_method[1] == 1,
		 * all fingerprints are reserved in memory.
		 * Thus container prefetching is unnecessary.
		 */
		container_meta_cache = new_lru_cache(destor.index_container_cache_size,
				free_container_meta, lookup_fingerprint_in_container_meta);

	}
}

void close_near_exact_locality_index() {
	if (cid != TEMPORARY_ID) {
		GQueue *features = featuring(NULL, 1);
		fingerprint *feature = g_queue_pop_head(features);
		do {
			feature_index_update(feature, cid);
			free(feature);
		} while ((feature = g_queue_pop_head(features)));
		g_queue_free(features);
	}

	close_feature_index();

	if (container_meta_cache)
		free_lru_cache(container_meta_cache);

}

void near_exact_locality_index_lookup(struct segment* s) {

	/*
	 * In the category,
	 * the notion of segment is only for batch process,
	 * not for similarity detection.
	 */
	struct segment *bs = new_segment();

	int len = g_queue_get_length(s->chunks), i;

	for (i = 0; i < len; ++i) {
		struct chunk* c = g_queue_peek_nth(s->chunks, i);

		if (c->size < 0)
			continue;

		GQueue *tq = g_hash_table_lookup(index_buffer.table, &c->fp);
		if (tq) {
			struct indexElem *be = g_queue_peak_head(tq);
			c->id = be->id;
			c->flag |= CHUNK_DUPLICATE;
		} else {
			tq = g_queue_new();
		}

		if (CHECK_CHUNK_UNIQUE(c) && container_meta_cache) {
			struct containerMeta* cm = lru_cache_lookup(container_meta_cache,
					c->fp);
			if (cm) {
				/* Find it */
				c->flag |= CHUNK_DUPLICATE;
				c->id = cm->id;
			}
		}

		if (CHECK_CHUNK_UNIQUE(c)) {
			GQueue *ids = feature_index_lookup(&c->fp);

			if (ids) {
				containerid *id = g_queue_peak_tail(ids);
				/* Find it */
				c->flag |= CHUNK_DUPLICATE;
				c->id = *id;

				if (container_meta_cache) {
					struct containerMeta * cm = retrieve_container_meta_by_id(
							c->id);
					lru_cache_insert(container_meta_cache, cm, NULL, NULL);
				}
			}
		}

		struct indexElem *ne = (struct indexElem*) malloc(
				sizeof(struct indexElem));
		ne->id = c->id;
		memcpy(&ne->fp, &c->fp, sizeof(fingerprint));

		g_queue_push_tail(bs->chunks, ne);
		g_queue_push_tail(tq, ne);
		g_hash_table_replace(index_buffer.table, &ne->fp, tq);

	}

	g_queue_push_tail(index_buffer.segment_queue, bs);
}

containerid near_exact_locality_index_update(fingerprint fp, containerid from,
		containerid to) {
	static int n = 0;

	containerid final_id = TEMPORARY_ID;

	struct segment* bs = g_queue_peak_head(index_buffer.segment_queue); // current segment

	struct indexElem* e = g_queue_peak_nth(bs->chunks, n++); // current chunk

	assert(from >= to);
	assert(e->id >= from);
	assert(g_fingerprint_equal(&fp, &e->fp));
	assert(g_queue_peak_head(g_hash_table_lookup(&fp)) == e);

	if (from < e->id) {
		/* to is meaningless. */
		final_id = e->id;
	} else {

		if (from != to) {

			if (cid != TEMPORARY_ID && cid != to) {
				/* Another container */
				GHashTable *features = featuring(NULL, 1);

				GHashTableIter iter;
				fingerprint *feature, *value;
				g_hash_table_iter_init(&iter, features);
				while (g_hash_table_iter_next(&iter, &feature, &value))
					feature_index_update(feature, cid);

				g_hash_table_destroy(features);
			}

			cid = to;
			featuring(fp, 0);

			GQueue *tq = g_hash_table_lookup(&e->fp);
			assert(tq);

			int len = g_queue_get_length(tq), i;
			for (i = 0; i < len; i++) {
				struct indexElem* ue = g_queue_peak_nth(tq, i);
				ue->id = to;
			}
		} else {
			/* a normal redundant chunk */
		}
	}

	if (n == g_queue_get_length(bs->chunks)) {
		/*
		 * Current segment is finished.
		 * We remove it from buffer.
		 * */
		bs = g_queue_pop_head(index_buffer.segment_queue);

		struct indexElem* ee = g_queue_pop_head(bs->chunks);
		do {
			GQueue *tq = g_hash_table_lookup(index_buffer.table, &ee->fp);
			assert(g_queue_peak_head(tq) == ee);
			g_queue_pop_head(tq);
			if (g_queue_get_length(tq) == 0) {
				/* tp is freed by hash table automatically. */
				g_hash_table_remove(index_buffer.table, &ee->fp);
			}
			free(ee);
		} while ((ee = g_queue_pop_head(bs->chunks)));

		free_segment(bs, free);
	}

	return final_id;
}