// SPDX-License-Identifier: GPL-2.0-only
/*
 * Copyright (C) 2024  Alviro Iskandar Setiawan <alviro.iskandar@gnuweeb.org>
 *
 * Biznet data leak scraper.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 *
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdint.h>
#include <errno.h>
#include <stdbool.h>
#include <pthread.h>
#include <curl/curl.h>
#include <sys/signal.h>

#define NUM_WORKERS	32
#define MAX_JOBS	1024
#define JOBS_MASK	(MAX_JOBS - 1)

#ifndef ARRAY_SIZE
#define ARRAY_SIZE(x) (sizeof(x) / sizeof((x)[0]))
#endif

#ifdef __CHECKER__
#define __must_hold(x) __attribute__((__context__(x)))
#else
#define __must_hold(x)
#endif

static volatile bool *g_stop_me;

struct curl_data {
	char		*data;
	size_t		size;
};

enum {
	WAKE_UP_PRODUCER_WHEN_POOL_IS_EMPTY     = 1,
	WAKE_UP_PRODUCER_WHEN_POOL_IS_AVAILABLE = 2
};

enum {
	JOB_TYPE_SCRAPE_TABLE = 0
};

struct job_scrape_table {
	char			tb_name[32];
	unsigned long long	limit;
	unsigned long long	offset;
};

struct job {
	uint8_t			type;
	union {
		struct job_scrape_table	scrape_table;
	};
};

struct gwbiz_wrk;
struct gwbiz_ctx;

struct gwbiz_wrk {
	pthread_t		thread;
	struct gwbiz_ctx	*ctx;
	CURL			*curl;
	bool			has_job;
	struct job		job;
	struct curl_data	cdata;
	uint32_t		idx;
};

struct gwbiz_ctx {
	volatile bool		stop_me;
	uint8_t			producer_wake_flag;
	struct gwbiz_wrk	workers[NUM_WORKERS];
	pthread_mutex_t		lock;
	pthread_cond_t		consumer_cond;
	pthread_cond_t		producer_cond;
	uint32_t		head;
	uint32_t		tail;
	uint32_t		mask;
	struct job		job_pool[MAX_JOBS];
};


static void gwbiz_sighandler(int sig)
{
	(void)sig;
	if (g_stop_me && !*g_stop_me) {
		puts("\nStopping all workers...");
		*g_stop_me = true;
	}
}

static void gwbiz_set_sighandler(struct gwbiz_ctx *ctx)
{
	struct sigaction sa = { .sa_handler = gwbiz_sighandler };

	g_stop_me = &ctx->stop_me;
	sigaction(SIGINT, &sa, NULL);
	sigaction(SIGTERM, &sa, NULL);
}

static bool producer_need_wake_up(struct gwbiz_ctx *ctx)
{
	if (ctx->producer_wake_flag == 0)
		return false;

	if (ctx->producer_wake_flag == WAKE_UP_PRODUCER_WHEN_POOL_IS_AVAILABLE)
		return ctx->head - ctx->tail < MAX_JOBS;

	if (ctx->producer_wake_flag == WAKE_UP_PRODUCER_WHEN_POOL_IS_EMPTY)
		return ctx->head == ctx->tail;

	return false;
}

static int __gwbiz_wait_for_job(struct gwbiz_ctx *ctx)
	__must_hold(ctx->lock)
{
	while (ctx->head == ctx->tail) {
		pthread_cond_wait(&ctx->consumer_cond, &ctx->lock);

		/*
		 * Must stop waiting if we are told to stop.
		 */
		if (ctx->stop_me)
			return -1;
	}

	return 0;
}

static int __gwbiz_get_job(struct gwbiz_wrk *wrk)
	__must_hold(wrk->ctx->lock)
{
	struct gwbiz_ctx *ctx = wrk->ctx;

	if (ctx->head == ctx->tail)
		return -1;

	wrk->job = ctx->job_pool[ctx->head++ & ctx->mask];
	wrk->has_job = true;
	if (producer_need_wake_up(ctx))
		pthread_cond_signal(&ctx->producer_cond);

	return 0;
}

static size_t __gwbiz_curl_write_cb(char *ptr, size_t size, size_t nmemb, void *userdata)
{
	struct curl_data *cdata = userdata;
	size_t realsize = size * nmemb;
	char *newptr;

	newptr = realloc(cdata->data, cdata->size + realsize + 1);
	if (!newptr) {
		free(cdata->data);
		cdata->data = NULL;
		cdata->size = 0;
		return 0;
	}

	cdata->data = newptr;
	memcpy(&cdata->data[cdata->size], ptr, realsize);
	cdata->size += realsize;
	cdata->data[cdata->size] = '\0';
	return realsize;
}

static int __gwbiz_scrape_table(struct gwbiz_wrk *wrk, struct job_scrape_table *job)
{
	static const char api_url[] = "http://wa7qbwp72rjvo6ocx4lwbhnmhsqrw4lv6tfipnhoeubebl4jo2o2izyd.onion/api2.php?table=%s&limit=%llu&offset=%llu";
	CURL *curl = wrk->curl;
	long http_code = 0;
	char url[1024];
	CURLcode res;

	snprintf(url, sizeof(url), api_url, job->tb_name, job->limit, job->offset);

	wrk->cdata.data = NULL;
	wrk->cdata.size = 0;
	curl_easy_setopt(curl, CURLOPT_URL, url);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, __gwbiz_curl_write_cb);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &wrk->cdata);
	curl_easy_setopt(curl, CURLOPT_USERAGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36");

	printf("Fetching %s\n", url);
	res = curl_easy_perform(curl);
	if (res != CURLE_OK) {
		fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));
		goto out_err;
	}

	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
	if (http_code != 200) {
		fprintf(stderr, "HTTP response code: %ld (expected 200, url = %s)\n", http_code, url);
		goto out_err;
	}

	return 0;

out_err:
	free(wrk->cdata.data);
	wrk->cdata.data = NULL;
	wrk->cdata.size = 0;
	return -1;
}

static int gwbiz_save_table(struct gwbiz_wrk *wrk, struct job_scrape_table *job)
{
	char filename[128];
	FILE *fp;

	snprintf(filename, sizeof(filename), "./data/%s-%llu-%llu.json", job->tb_name, job->limit, job->offset);
	fp = fopen(filename, "wb");
	if (!fp) {
		printf("Failed to open %s for writing: %s\n", filename, strerror(errno));
		return -1;
	}

	fwrite(wrk->cdata.data, 1, wrk->cdata.size, fp);
	fclose(fp);
	return 0;
}

static int gwbiz_scrape_table(struct gwbiz_wrk *wrk, struct job_scrape_table *job)
{
	uint32_t try_count = 0;
	int ret;

	do {
		ret = __gwbiz_scrape_table(wrk, job);
		if (!ret)
			break;

		printf("Failed to fetch (tb_name = %s, limit = %llu, offset = %llu), retrying [try_count=%u]...\n",
			job->tb_name, job->limit, job->offset, try_count);
		sleep(1);
	} while (++try_count < 3);

	if (ret)
		return -1;

	if (gwbiz_save_table(wrk, job))
		return -1;

	free(wrk->cdata.data);
	wrk->cdata.data = NULL;
	wrk->cdata.size = 0;
	return 0;
}

static int gwbiz_execute_job(struct gwbiz_wrk *wrk)
{
	struct job *job = &wrk->job;
	int ret = 0;

	switch (job->type) {
	case JOB_TYPE_SCRAPE_TABLE:
		ret = gwbiz_scrape_table(wrk, &job->scrape_table);
		break;
	default:
		printf("Unknown job type: %d\n", job->type);
		break;
	}

	wrk->has_job = false;
	return ret;
}

static void gwbiz_stop_workers(struct gwbiz_ctx *ctx)
{
	pthread_mutex_lock(&ctx->lock);
	ctx->stop_me = true;
	pthread_cond_broadcast(&ctx->consumer_cond);
	if (producer_need_wake_up(ctx))
		pthread_cond_signal(&ctx->producer_cond);
	pthread_mutex_unlock(&ctx->lock);
}

static void *run_worker(void *wrk_)
{
	struct gwbiz_wrk *wrk = wrk_;
	struct gwbiz_ctx *ctx = wrk->ctx;

	pthread_mutex_lock(&ctx->lock);
	while (1) {
		int ret;

		if (ctx->stop_me)
			break;

		if (__gwbiz_wait_for_job(ctx))
			break;

		if (__gwbiz_get_job(wrk))
			continue;

		pthread_mutex_unlock(&ctx->lock);
		ret = gwbiz_execute_job(wrk);
		pthread_mutex_lock(&ctx->lock);
		if (ret)
			break;
	}

	if (ctx->producer_wake_flag != 0)
		pthread_cond_signal(&ctx->producer_cond);
	pthread_mutex_unlock(&ctx->lock);

	printf("Worker %u stopped!\n", wrk->idx);
	gwbiz_stop_workers(ctx);
	return NULL;
}

static int gwbiz_wrk_init(struct gwbiz_wrk *wrk)
{
	char proxy[128];
	int ret;

	wrk->curl = curl_easy_init();
	if (!wrk->curl)
		return -1;

	ret = pthread_create(&wrk->thread, NULL, run_worker, wrk);
	if (ret) {
		curl_easy_cleanup(wrk->curl);
		wrk->curl = NULL;
		return ret;
	}

	snprintf(proxy, sizeof(proxy), "socks5h://68.183.184.174:%u", 63400 + wrk->idx);
	curl_easy_setopt(wrk->curl, CURLOPT_PROXY, proxy);
	return 0;
}

static void gwbiz_wrk_fini(struct gwbiz_wrk *wrk)
{
	pthread_join(wrk->thread, NULL);
	curl_easy_cleanup(wrk->curl);
}

static int gwbiz_workers_init(struct gwbiz_ctx *ctx)
{
	size_t i;
	int ret;

	for (i = 0; i < ARRAY_SIZE(ctx->workers); i++) {
		ctx->workers[i].ctx = ctx;
		ctx->workers[i].idx = i;
		ret = gwbiz_wrk_init(&ctx->workers[i]);
		if (ret)
			goto err;
	}
	return 0;

err:
	gwbiz_stop_workers(ctx);
	while (i--)
		gwbiz_wrk_fini(&ctx->workers[i]);

	return ret;
}

static void gwbiz_workers_fini(struct gwbiz_ctx *ctx)
{
	size_t i;

	gwbiz_stop_workers(ctx);
	for (i = 0; i < ARRAY_SIZE(ctx->workers); i++)
		gwbiz_wrk_fini(&ctx->workers[i]);
}

static struct gwbiz_ctx *gwbiz_ctx_init(void)
{
	struct gwbiz_ctx *ctx;
	int ret;

	ctx = calloc(1, sizeof(*ctx));
	if (!ctx)
		return NULL;

	if (pthread_mutex_init(&ctx->lock, NULL)) {
		free(ctx);
		return NULL;
	}

	if (pthread_cond_init(&ctx->consumer_cond, NULL)) {
		pthread_mutex_destroy(&ctx->lock);
		free(ctx);
		return NULL;
	}

	if (pthread_cond_init(&ctx->producer_cond, NULL)) {
		pthread_mutex_destroy(&ctx->lock);
		pthread_cond_destroy(&ctx->consumer_cond);
		free(ctx);
		return NULL;
	}

	ret = gwbiz_workers_init(ctx);
	if (ret) {
		pthread_mutex_destroy(&ctx->lock);
		pthread_cond_destroy(&ctx->consumer_cond);
		pthread_cond_destroy(&ctx->producer_cond);
		free(ctx);
		return NULL;
	}

	gwbiz_set_sighandler(ctx);
	ctx->mask = JOBS_MASK;
	ctx->head = ctx->tail = 0;
	return ctx;
}

static int __gwbiz_produce_job(struct gwbiz_ctx *ctx, struct job *job)
	__must_hold(ctx->lock)
{
	if ((ctx->head - ctx->tail) >= MAX_JOBS)
		return -1;

	ctx->job_pool[ctx->tail++ & ctx->mask] = *job;
	return 0;
}

static int __gwbiz_wait_for_job_pool_free(struct gwbiz_ctx *ctx)
	__must_hold(ctx->lock)
{
	while ((ctx->head - ctx->tail) >= MAX_JOBS) {
		pthread_cond_broadcast(&ctx->consumer_cond);

		ctx->producer_wake_flag = WAKE_UP_PRODUCER_WHEN_POOL_IS_AVAILABLE;
		pthread_cond_wait(&ctx->producer_cond, &ctx->lock);
		ctx->producer_wake_flag = 0;

		/*
		 * Must stop waiting if we are told to stop.
		 */
		if (ctx->stop_me)
			return -1;
	}

	return 0;
}

static int __gwbiz_wait_for_job_pool_be_empty(struct gwbiz_ctx *ctx)
	__must_hold(ctx->lock)
{
	while (ctx->head != ctx->tail) {

		ctx->producer_wake_flag = WAKE_UP_PRODUCER_WHEN_POOL_IS_EMPTY;
		pthread_cond_wait(&ctx->producer_cond, &ctx->lock);
		ctx->producer_wake_flag = 0;

		/*
		 * Must stop waiting if we are told to stop.
		 */
		if (ctx->stop_me)
			return -1;
	}

	return 0;
}

struct table_to_fetch {
	char tb_name[32];
	unsigned long long limit;
	unsigned long long start_offset;
	unsigned long long end_offset;
};

static int gwbiz_run(struct gwbiz_ctx *ctx)
{
	static const struct table_to_fetch tables[] = {
		{
			.tb_name = "Customers",
			.limit = 5000,
			.start_offset = 0,
			.end_offset = 381132
		},
		{
			.tb_name = "Addresses",
			.limit = 5000,
			.start_offset = 0,
			.end_offset = 1152563
		},
		{
			.tb_name = "ContractAccounts",
			.limit = 5000,
			.start_offset = 0,
			.end_offset = 388935
		},
		{
			.tb_name = "Contracts",
			.limit = 5000,
			.start_offset = 0,
			.end_offset = 391072
		},
		{
			.tb_name = "Products",
			.limit = 5000,
			.start_offset = 0,
			.end_offset = 801205
		}
	};
	struct job jobs[ARRAY_SIZE(tables)];
	size_t i;

	for (i = 0; i < ARRAY_SIZE(tables); i++) {
		jobs[i].type = JOB_TYPE_SCRAPE_TABLE;
		memcpy(jobs[i].scrape_table.tb_name, tables[i].tb_name, sizeof(jobs[i].scrape_table.tb_name));
		jobs[i].scrape_table.limit = tables[i].limit;
		jobs[i].scrape_table.offset = tables[i].start_offset;
	}

	pthread_mutex_lock(&ctx->lock);
	while (!ctx->stop_me) {
		bool still_has_jobs = false;

		for (i = 0; i < ARRAY_SIZE(tables); i++) {
			if (jobs[i].scrape_table.offset >= tables[i].end_offset)
				continue;

			still_has_jobs = true;
			if (__gwbiz_wait_for_job_pool_free(ctx))
				break;

			if (__gwbiz_produce_job(ctx, &jobs[i]))
				break;

			jobs[i].scrape_table.offset += jobs[i].scrape_table.limit;
		}

		if (!still_has_jobs) {
			__gwbiz_wait_for_job_pool_be_empty(ctx);
			break;
		}
	}
	pthread_mutex_unlock(&ctx->lock);

	return 0;
}

static void gwbiz_ctx_fini(struct gwbiz_ctx *ctx)
{
	gwbiz_workers_fini(ctx);
	pthread_mutex_destroy(&ctx->lock);
	pthread_cond_destroy(&ctx->consumer_cond);
	pthread_cond_destroy(&ctx->producer_cond);
	free(ctx);
}

int main(void)
{
	struct gwbiz_ctx *ctx;
	int ret;

	ctx = gwbiz_ctx_init();
	if (!ctx)
		return 1;

	ret = gwbiz_run(ctx);
	gwbiz_ctx_fini(ctx);
	return ret;
}
