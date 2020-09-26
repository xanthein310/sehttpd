/*
 * Copyright (c) 2016, Mathias Brossard <mathias@brossard.org>.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *  1. Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * @file threadpool.c
 * @brief Threadpool implementation file
 */

#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

#include "threadpool.h"

typedef enum {
    immediate_shutdown = 1,
    graceful_shutdown = 2
} threadpool_shutdown_t;

/**
 *  @struct threadpool_task
 *  @brief the work struct
 *
 *  @var function Pointer to the function that will perform the task.
 *  @var argument Argument to be passed to the function.
 */

typedef struct {
    void (*function)(void *);
    void *argument;
} threadpool_task_t;

typedef struct {
    threadpool_task_t *queue;
    int head;
    int tail;
    volatile int count;
} thread_t;

/**
 *  @struct threadpool
 *  @brief The threadpool struct
 *
 *  @var notify       Condition variable to notify worker threads.
 *  @var threads      Array containing worker threads ID.
 *  @var thread_count Number of threads
 *  @var queue        Array containing the task queue.
 *  @var queue_size   Size of the task queue.
 *  @var head         Index of the first element.
 *  @var tail         Index of the next element.
 *  @var count        Number of pending tasks
 *  @var shutdown     Flag indicating if the pool is shutting down
 *  @var started      Number of started threads
 */
struct threadpool_t {
    pthread_t *threads;
    int thread_count;
    int queue_size;
    int started;
    int shutdown;
    thread_t *thread_data;
};

/**
 * @function void *threadpool_thread(void *threadpool)
 * @brief the worker thread
 * @param threadpool the pool which own the thread
 */
static void *threadpool_thread(void *threadpool);

int threadpool_free(threadpool_t *pool);

static void sig_do_nothing(int signo)
{
    return;
}

threadpool_t *threadpool_create(int thread_count, int queue_size, int flags)
{
    threadpool_t *pool;
    (void) flags;

    if (thread_count <= 0 || thread_count > MAX_THREADS || queue_size <= 0 ||
        queue_size > MAX_QUEUE) {
        return NULL;
    }

    if ((pool = (threadpool_t *) malloc(sizeof(threadpool_t))) == NULL) {
        goto err;
    }

    /* Initialize */
    pool->thread_count = 0;
    pool->queue_size = queue_size;
    pool->shutdown = pool->started = 0;

    /* Allocate thread and task queue */
    pool->threads = (pthread_t *) malloc(sizeof(pthread_t) * thread_count);
    pool->thread_data = (thread_t *) malloc(sizeof(thread_t) * thread_count);
    for (int i = 0; i < thread_count; i++) {
        pool->thread_data[i].queue = (threadpool_task_t *) malloc(
            sizeof(threadpool_task_t) * queue_size);
        pool->thread_data[i].head = 0;
        pool->thread_data[i].tail = 0;
        pool->thread_data[i].count = 0;
    }

    if (signal(SIGUSR1, sig_do_nothing) == SIG_ERR)
        printf("signal failed");
    /* Initialize mutex and conditional variable first */
    if ((pool->threads == NULL) || (pool->thread_data == NULL) ||
        (pool->thread_data->queue == NULL)) {
        goto err;
    }

    /* Start worker threads */
    for (int i = 0; i < thread_count; i++) {
        if (pthread_create(&(pool->threads[i]), NULL, threadpool_thread,
                           (void *) pool) != 0) {
            threadpool_destroy(pool, 0);
            return NULL;
        }
        pool->thread_count++;
        pool->started++;
    }

    return pool;

err:
    if (pool) {
        threadpool_free(pool);
    }
    return NULL;
}

int threadpool_add(threadpool_t *pool,
                   void (*function)(void *),
                   void *argument,
                   int flags)
{
    int err = 0;
    (void) flags;

    static int cur_thread_index = -1;

    if (pool == NULL || function == NULL) {
        return threadpool_invalid;
    }

    do {
        cur_thread_index = (cur_thread_index + 1) % pool->thread_count;
        thread_t *thread_data = pool->thread_data + cur_thread_index;
        int next = (thread_data->tail + 1) % pool->queue_size;

        /* Are we full ? */
        if (__sync_bool_compare_and_swap(&(thread_data->count),
                                         pool->queue_size, pool->queue_size)) {
            continue;
        }

        /* Are we shutting down ? */
        if (pool->shutdown) {
            err = threadpool_shutdown;
            break;
        }

        /* Add task to queue */
        thread_data->queue[thread_data->tail].function = function;
        thread_data->queue[thread_data->tail].argument = argument;
        thread_data->tail = next;
        __sync_fetch_and_add(&(thread_data->count), 1);
        pthread_kill(pool->threads[cur_thread_index], SIGUSR1);
        break;
    } while (1);

    return err;
}

int threadpool_destroy(threadpool_t *pool, int flags)
{
    int i, err = 0;

    if (pool == NULL) {
        return threadpool_invalid;
    }

    do {
        /* Already shutting down */
        if (pool->shutdown) {
            err = threadpool_shutdown;
            break;
        }

        pool->shutdown = (flags & threadpool_graceful) ? graceful_shutdown
                                                       : immediate_shutdown;

        /* Join all worker thread */
        for (i = 0; i < pool->thread_count; i++) {
            pthread_kill(pool->threads[i], SIGUSR1);
            if (pthread_join(pool->threads[i], NULL) != 0) {
                err = threadpool_thread_failure;
            }
        }
    } while (0);

    /* Only if everything went well do we deallocate the pool */
    if (!err) {
        threadpool_free(pool);
    }
    return err;
}

int threadpool_free(threadpool_t *pool)
{
    if (pool == NULL || pool->started > 0) {
        return -1;
    }

    /* Did we manage to allocate ? */
    if (pool->threads) {
        free(pool->threads);
        free(pool->thread_data->queue);
        free(pool->thread_data);
    }
    free(pool);
    return 0;
}


static void *threadpool_thread(void *threadpool)
{
    threadpool_t *pool = (threadpool_t *) threadpool;
    threadpool_task_t task;
    thread_t *thread_data;
    sigset_t newmask, oldmask;
    int sig_caught;

    for (int i = 0; i < pool->thread_count; i++) {
        if (pool->threads[i] == pthread_self()) {
            thread_data = pool->thread_data + i;
            break;
        }
    }

    sigemptyset(&oldmask);
    sigemptyset(&newmask);
    sigaddset(&newmask, SIGUSR1);

    for (;;) {
        if (pthread_sigmask(SIG_BLOCK, &newmask, &oldmask) != 0)
            printf("set SIG_BLOCK failed\n");

        while (__sync_bool_compare_and_swap(&(thread_data->count), 0, 0) &&
               !pool->shutdown) {
            int rc = sigwait(&newmask, &sig_caught);
            if (rc != 0)
                printf("sigwait failed\n");
        }

        if (pthread_sigmask(SIG_SETMASK, &oldmask, NULL) != 0)
            printf("set SIG_BLOCK failed\n");

        if ((pool->shutdown == immediate_shutdown) ||
            ((pool->shutdown == graceful_shutdown) &&
             __sync_bool_compare_and_swap(&(thread_data->count), 0, 0))) {
            break;
        }

        /* Grab our task */
        task.function = thread_data->queue[thread_data->head].function;
        task.argument = thread_data->queue[thread_data->head].argument;
        thread_data->head = (thread_data->head + 1) % pool->queue_size;
        __sync_fetch_and_sub(&(thread_data->count), 1);

        /* Get to work */
        (*(task.function))(task.argument);
    }

    __sync_fetch_and_sub(&pool->started, 1);

    pthread_exit(NULL);

    return (NULL);
}
