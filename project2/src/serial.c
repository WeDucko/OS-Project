// Project 2
// Group 30
// Martin Ganen-Villa U49246681, Jacob Cooksey, Nicholas Keenan, Riley Langworhy

#include <dirent.h> 
#include <stdio.h> 
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>

#define BUFFER_SIZE 1048576 // 1MB

static int endsWithTxt(const char *name) {
	size_t len = strlen(name);
	if (len < 4) {
		return 0;
	}
	
	return (name[len-4]=='.' && name[len-3]=='t' && name[len-2]=='x' && name[len-1]=='t');
}



int cmp(const void *a, const void *b) {
	return strcmp(*(char **) a, *(char **) b);
}

typedef struct {
	unsigned char *buf;
	int zip_size;
	int in_size;
	int ready;
	pthread_mutex_t m;
	pthread_cond_t cv;
} result_t;

static void result_init(result_t *r) {
	r->buf = NULL;
	r->zip_size = 0;
	r->in_size = 0;
	r->ready = 0;
	pthread_mutex_init(&r->m, NULL);
	pthread_cond_init(&r->cv, NULL);
}

static void result_destroy(result_t *r) {
	if (r->buf) free(r->buf);
	pthread_cond_destroy(&r->cv);
	pthread_mutex_destroy(&r->m);
}

typedef struct {
	int *items;
	int cap, head, tail, count;
	int closed;
	pthread_mutex_t m;
	pthread_cond_t not_empty, not_full;
} queue_t;


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

static void queue_close(queue_t *q) {
	pthread_mutex_lock(&q->m);
	q->closed = 1;
	pthread_cond_broadcast(&q->not_empty);
	pthread_mutex_unlock(&q->m);
}

static int queue_destroy(queue_t *q) {
	free(q->items);
	pthread_cond_destroy(&q->not_empty);
	pthread_cond_destroy(&q->not_full);
	pthread_mutex_destroy(&q->m);
}

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
	q->tail = (q->tail + 1);
	q->count++;
	pthread_cond_signal(&q->not_empty);
	pthread_mutex_unlock(&q->m);
	return 0;
}

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
	pthread_mutex_unlock(&Q->m);
	return 0;

}

typedef struct {
	queue_t *queue;
	char **files;
	const char *directory;
	result_t results;
} worker_arg_t;

static void *worker(void *arg) {
	worker_arg_t *wa = (worker_arg_t *)arg;
	queue_t *q = wa->queue;
	char **files = wa->files;
	const char *directory = wa->directory;
	result_t *results = wa->results;

	for (;;) {
		int idx;
		if (queue_pop(q, &idx) != 0) {
			break;
		}

		int len = (int)strlen(directory) + (int)strlen(files[idx]) + 2;
		char *full_path = (char *)malloc(len);
		assert(full_path != NULL);

		strcpy(full_path, directory);
		strcat(full_path, "/");
		strcat(full_path, files[idx]);
		
	}
}

int compress_directory(char *directory_name) {
	DIR *d;
	struct dirent *dir;
	char **files = NULL;
	int nfiles = 0;

	d = opendir(directory_name);
	if(d == NULL) {
		printf("An error has occurred\n");
		return 0;
	}

	// create sorted list of text files
	while ((dir = readdir(d)) != NULL) {
		files = realloc(files, (nfiles+1)*sizeof(char *));
		assert(files != NULL);

		int len = strlen(dir->d_name);
		if(dir->d_name[len-4] == '.' && dir->d_name[len-3] == 't' && dir->d_name[len-2] == 'x' && dir->d_name[len-1] == 't') {
			files[nfiles] = strdup(dir->d_name);
			assert(files[nfiles] != NULL);

			nfiles++;
		}
	}
	closedir(d);
	qsort(files, nfiles, sizeof(char *), cmp);

	// create a single zipped package with all text files in lexicographical order
	int total_in = 0, total_out = 0;
	FILE *f_out = fopen("text.tzip", "w");
	assert(f_out != NULL);
	int i = 0;
	for(i=0; i < nfiles; i++) {
		int len = strlen(directory_name)+strlen(files[i])+2;
		char *full_path = malloc(len*sizeof(char));
		assert(full_path != NULL);
		strcpy(full_path, directory_name);
		strcat(full_path, "/");
		strcat(full_path, files[i]);

		unsigned char buffer_in[BUFFER_SIZE];
		unsigned char buffer_out[BUFFER_SIZE];

		// load file
		FILE *f_in = fopen(full_path, "r");
		assert(f_in != NULL);
		int nbytes = fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
		fclose(f_in);
		total_in += nbytes;

		// zip file
		z_stream strm;
		int ret = deflateInit(&strm, 9);
		assert(ret == Z_OK);
		strm.avail_in = nbytes;
		strm.next_in = buffer_in;
		strm.avail_out = BUFFER_SIZE;
		strm.next_out = buffer_out;

		ret = deflate(&strm, Z_FINISH);
		assert(ret == Z_STREAM_END);

		// dump zipped file
		int nbytes_zipped = BUFFER_SIZE-strm.avail_out;
		fwrite(&nbytes_zipped, sizeof(int), 1, f_out);
		fwrite(buffer_out, sizeof(unsigned char), nbytes_zipped, f_out);
		total_out += nbytes_zipped;

		free(full_path);
	}
	fclose(f_out);

	printf("Compression rate: %.2lf%%\n", 100.0*(total_in-total_out)/total_in);

	// release list of files
	for(i=0; i < nfiles; i++)
		free(files[i]);
	free(files);

	// do not modify the main function after this point!
	return 0;
}
