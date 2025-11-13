// Project 2 - Parallel Text Compression
// Group 30
// Martin Ganen-Villa U49246681, Jacob Cooksey U39145995, Nicholas Keenan U63119825, Riley Langworhy U44956927
// This program parallelizes text file compression using a producer-consumer pattern
// with worker threads that compress files concurrently while maintaining output order

#include <dirent.h> 
#include <stdio.h> 
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>

#define BUFFER_SIZE 1048576 // 1MB buffer for file I/O
#define NUM_THREADS 8 // Number of worker threads for parallel compression
#define QUEUE_SIZE 100 // Maximum queue capacity for work items

// Compare function for qsort
int cmp(const void *a, const void *b) {
	return strcmp(*(char **) a, *(char **) b);
}

// Structure to hold compression result for each file
typedef struct {
	unsigned char *buf;
	int zip_size;
	int in_size;
	int ready;
	pthread_mutex_t m;
	pthread_cond_t cv;
} result_t;

// Thread-safe queue structure
typedef struct {
	int *items;
	int cap, head, tail, count;
	int closed;
	pthread_mutex_t m;
	pthread_cond_t not_empty, not_full;
} queue_t;

// Initialize queue
static void queue_init(queue_t *q, int cap) {
	q->items = (int*)malloc(sizeof(int)*cap);
	assert(q->items != NULL);
	q->cap = cap;
	q->head = 0;
	q->tail = 0;
	q->count = 0;
	q->closed = 0;
	pthread_mutex_init(&q->m, NULL);
	pthread_cond_init(&q->not_empty, NULL);
	pthread_cond_init(&q->not_full, NULL);
}

// Close queue
static void queue_close(queue_t *q) {
	pthread_mutex_lock(&q->m);
	q->closed = 1;
	pthread_cond_broadcast(&q->not_empty);
	pthread_mutex_unlock(&q->m);
}

// Destroy queue
static void queue_destroy(queue_t *q) {
	if (q->items) {
		free(q->items);
	}
	pthread_cond_destroy(&q->not_empty);
	pthread_cond_destroy(&q->not_full);
	pthread_mutex_destroy(&q->m);
}

// Push item to queue
static int queue_push(queue_t *q, int v) {
	pthread_mutex_lock(&q->m);
	while (q->count == q->cap && !q->closed) {
		pthread_cond_wait(&q->not_full, &q->m);
	}
	if (q->closed) {
		pthread_mutex_unlock(&q->m);
		return -1;
	}
	q->items[q->tail] = v;
	q->tail = (q->tail + 1) % q->cap;
	q->count++;
	pthread_cond_signal(&q->not_empty);
	pthread_mutex_unlock(&q->m);
	return 0;
}

// Pop item from queue
static int queue_pop(queue_t *q, int *out) {
	pthread_mutex_lock(&q->m);
	while (q->count == 0 && !q->closed) {
		pthread_cond_wait(&q->not_empty, &q->m);
	}
	if (q->count == 0 && q->closed) {
		pthread_mutex_unlock(&q->m);
		return -1;
	}
	*out = q->items[q->head];
	q->head = (q->head + 1) % q->cap;
	q->count--;
	pthread_cond_signal(&q->not_full);
	pthread_mutex_unlock(&q->m);
	return 0;
}

// Worker thread argument structure
typedef struct {
	queue_t *queue;
	char **files;
	const char *directory;
	result_t *results;
} worker_arg_t;

// Worker thread function - processes compression tasks from the queue
static void *worker(void *arg) {
	worker_arg_t *wa = (worker_arg_t *)arg;
	
	for (;;) {
		int idx;
		// Get next file index from queue
		if (queue_pop(wa->queue, &idx) != 0) {
			break;  // Queue closed, exit thread
		}

		// Build full path
		int len = strlen(wa->directory)+strlen(wa->files[idx])+2;
		char *full_path = malloc(len*sizeof(char));
		if (!full_path) continue;
		strcpy(full_path, wa->directory);
		strcat(full_path, "/");
		strcat(full_path, wa->files[idx]);

		// Allocate buffers
		unsigned char *buffer_in = (unsigned char *)malloc(BUFFER_SIZE);
		unsigned char *buffer_out = (unsigned char *)malloc(BUFFER_SIZE);
		if (!buffer_in || !buffer_out) {
			if (buffer_in) free(buffer_in);
			if (buffer_out) free(buffer_out);
			free(full_path);
			continue;
		}

		// Load file
		FILE *f_in = fopen(full_path, "rb");
		if (!f_in) {
			free(buffer_in);
			free(buffer_out);
			free(full_path);
			continue;
		}
		int nbytes = fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
		fclose(f_in);

		// Compress file using zlib
		z_stream strm;
		strm.zalloc = Z_NULL;
		strm.zfree = Z_NULL;
		strm.opaque = Z_NULL;
		int ret = deflateInit(&strm, 9);
		if (ret != Z_OK) {
			free(buffer_in);
			free(buffer_out);
			free(full_path);
			continue;
		}
		
		strm.avail_in = nbytes;
		strm.next_in = buffer_in;
		strm.avail_out = BUFFER_SIZE;
		strm.next_out = buffer_out;
		ret = deflate(&strm, Z_FINISH);
		
		if (ret != Z_STREAM_END) {
			deflateEnd(&strm);
			free(buffer_in);
			free(buffer_out);
			free(full_path);
			continue;
		}

		int nbytes_zipped = BUFFER_SIZE - strm.avail_out;
		deflateEnd(&strm);

		// Store compressed result in results array with synchronization
		pthread_mutex_lock(&wa->results[idx].m);
		wa->results[idx].buf = (unsigned char *)malloc(nbytes_zipped);
		if (wa->results[idx].buf) {
			memcpy(wa->results[idx].buf, buffer_out, nbytes_zipped);
			wa->results[idx].zip_size = nbytes_zipped;
			wa->results[idx].in_size = nbytes;
			wa->results[idx].ready = 1;
		}
		pthread_cond_signal(&wa->results[idx].cv);
		pthread_mutex_unlock(&wa->results[idx].m);

		free(buffer_in);
		free(buffer_out);
		free(full_path);
	}
	return NULL;
}

// Main compression function
int compress_directory(char *directory_name) {
	DIR *d;
	struct dirent *dir;
	char **files = NULL;
	int nfiles = 0;
	int i;

	d = opendir(directory_name);
	if(d == NULL) {
		printf("An error has occurred\n");
		return 0;
	}

	// Create sorted list of text files
	while ((dir = readdir(d)) != NULL) {
		int len = strlen(dir->d_name);
		if(len >= 4 && dir->d_name[len-4] == '.' && dir->d_name[len-3] == 't' && 
		   dir->d_name[len-2] == 'x' && dir->d_name[len-1] == 't') {
			files = realloc(files, (nfiles+1)*sizeof(char *));
			assert(files != NULL);
			files[nfiles] = strdup(dir->d_name);
			assert(files[nfiles] != NULL);
			nfiles++;
		}
	}
	closedir(d);
	
	if (nfiles == 0) {
		if (files) free(files);
		return 0;
	}
	
	qsort(files, nfiles, sizeof(char *), cmp);

	// Initialize results array for storing compressed data
	result_t *results = (result_t *)malloc(nfiles * sizeof(result_t));
	assert(results != NULL);
	for (i = 0; i < nfiles; i++) {
		results[i].buf = NULL;
		results[i].zip_size = 0;
		results[i].in_size = 0;
		results[i].ready = 0;
		pthread_mutex_init(&results[i].m, NULL);
		pthread_cond_init(&results[i].cv, NULL);
	}

	// Create work queue for distributing files to worker threads
	queue_t queue;
	queue_init(&queue, QUEUE_SIZE);
	
	// Setup worker arguments
	pthread_t threads[NUM_THREADS];
	worker_arg_t wa;
	wa.queue = &queue;
	wa.files = files;
	wa.directory = directory_name;
	wa.results = results;

	// Start worker threads
	for (i = 0; i < NUM_THREADS; i++) {
		pthread_create(&threads[i], NULL, worker, &wa);
	}

	// Push all file indices to queue for processing
	for (i = 0; i < nfiles; i++) {
		queue_push(&queue, i);
	}
	queue_close(&queue);  // Signal no more work items

	// Write compressed files to output in lexicographical order
	int total_in = 0, total_out = 0;
	FILE *f_out = fopen("text.tzip", "wb");
	assert(f_out != NULL);
	
	for (i = 0; i < nfiles; i++) {
		// Wait for this file's compression to complete
		pthread_mutex_lock(&results[i].m);
		while (!results[i].ready) {
			pthread_cond_wait(&results[i].cv, &results[i].m);
		}
		pthread_mutex_unlock(&results[i].m);

		// Write compressed data to output file
		fwrite(&results[i].zip_size, sizeof(int), 1, f_out);
		fwrite(results[i].buf, sizeof(unsigned char), results[i].zip_size, f_out);
		total_in += results[i].in_size;
		total_out += results[i].zip_size;
	}
	fclose(f_out);

	printf("Compression rate: %.2lf%%\n", 100.0*(total_in-total_out)/total_in);

	// Wait for all worker threads to complete
	for (i = 0; i < NUM_THREADS; i++) {
		pthread_join(threads[i], NULL);
	}

	// Clean up allocated resources
	queue_destroy(&queue);
	for (i = 0; i < nfiles; i++) {
		if (results[i].buf) free(results[i].buf);
		pthread_cond_destroy(&results[i].cv);
		pthread_mutex_destroy(&results[i].m);
	}
	free(results);

	for(i=0; i < nfiles; i++)
		free(files[i]);
	free(files);

	return 0;
}