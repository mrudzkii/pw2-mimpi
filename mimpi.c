/**
 * This file is for implementation of MIMPI library.
 * */

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"
//#include <types.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>

static bool deadlock_detection;
bool performReading;
static int rank;
static int world_size;
bool isWaiting;
pthread_cond_t waiting;
pthread_mutex_t waitingMutex;
pthread_cond_t barrier;
pthread_mutex_t barrierMutex;
bool barrierUnlocked;
bool deadlockDetected;
int waitingForCount;
int waitingForTag;
int waitingForSource;

int* sendsReceives;

#define MAX_PROC 16

#define NORMAL_MESSAGE (u_int8_t)0

#define max(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })

struct buffElem {
    int tag;
    int source;
    int count;
    void* data;
    int written;
    struct buffElem * next;
    struct buffElem * prev;
};

struct buffElem * head;

struct buffElem* find(int source, int tag, int count){
    if(head == NULL){
        return NULL;
    }
    struct buffElem* elem = head;
    while (elem->next != NULL){
        elem = elem->next;
    }
    while(elem != NULL){
        if(elem->source == source && (elem->tag == tag || tag == MIMPI_ANY_TAG)  && elem->count == count)
            return elem;
        elem = elem->prev;
    }
    return elem;
}

void push(int source, int tag, int count){
    struct buffElem* new_elem = malloc(sizeof (struct buffElem));
    new_elem->source = source;
    new_elem->tag = tag;
    new_elem->count = count;
    new_elem->written = 0;
    new_elem->data = malloc((count/(512-sizeof (struct Message)) + 1)*(512-sizeof (struct Message))); //XDDDDDDDDDDD
    new_elem->next = head;
    new_elem->prev = NULL;
    if (head != NULL) {
        head->prev = new_elem;
    }
    head = new_elem;
}

void* reader(){
    struct Message *incomingMess;
    incomingMess = malloc(sizeof(struct Message));
    void* bcast_buff = malloc(512);
    int cur_written;
    while (performReading){
        ASSERT_SYS_OK(chrecv(20 + 2*rank, incomingMess, sizeof(struct Message)));
        ASSERT_SYS_OK(pthread_mutex_lock(&waitingMutex));
        if(incomingMess->tag == MIMPI_BARRIER_WAKE){
            barrierUnlocked = true;
            ASSERT_SYS_OK(pthread_cond_signal(&barrier));
            ASSERT_SYS_OK(pthread_mutex_unlock(&waitingMutex));
            continue;
        }
        if(incomingMess->tag == MIMPI_RECEIVED){
            sendsReceives[incomingMess->source]--;
            if(isWaiting && waitingForSource == incomingMess->source && sendsReceives[incomingMess->source] < 0){
                deadlockDetected = true;
                ASSERT_SYS_OK(pthread_cond_signal(&waiting));
            }
            ASSERT_SYS_OK(pthread_mutex_unlock(&waitingMutex));
            continue;
        }
        if(incomingMess->tag == MIMPI_DEADLOCK_DETECTED){
            deadlockDetected = true;
            ASSERT_SYS_OK(pthread_cond_signal(&waiting));
            ASSERT_SYS_OK(pthread_mutex_unlock(&waitingMutex));
            continue;
        }
        struct buffElem* elem = find(incomingMess->source, incomingMess->tag, incomingMess->count);
        if(elem == NULL){
            push(incomingMess->source, incomingMess->tag, incomingMess->count);
            elem = head;
            ASSERT_SYS_OK(cur_written = chrecv(20 + 2*rank, elem->data, 512 - sizeof(struct Message)));
            elem->written += cur_written;
            if(elem->written >= incomingMess->count
               && waitingForSource == elem->source && waitingForCount == elem->count && (waitingForTag == elem->tag || waitingForTag == MIMPI_ANY_TAG)
                    ) {
                ASSERT_SYS_OK(pthread_cond_signal(&waiting));
            }
            if(deadlock_detection){
                sendsReceives[incomingMess->source]++;
            }
        } else{
            ASSERT_SYS_OK(cur_written = chrecv(20 + 2*rank, elem->data + elem->written, 512 - sizeof(struct Message)));
            elem->written += cur_written;
            if(elem->written >= elem->count && waitingForSource == elem->source
                && waitingForCount == elem->count && (waitingForTag == elem->tag || waitingForTag == MIMPI_ANY_TAG)){
                ASSERT_SYS_OK(pthread_cond_signal(&waiting));
            }
        }
        if(incomingMess->tag == MIMPI_BCAST){
            memmove(bcast_buff, incomingMess, sizeof(struct Message));
            memmove(bcast_buff + sizeof(struct Message), elem->data + elem->written - cur_written, cur_written);
            if(2*(rank+2) - 2 < world_size && 2*(rank+2) - 2 != incomingMess->source) {
                ASSERT_SYS_OK(chsend(21 + 2 * rank + 2 * (rank + 2), bcast_buff, 512));
            }
            if(2*(rank+2) - 1 < world_size && 2*(rank+2) - 2 != incomingMess->source) {
                ASSERT_SYS_OK(chsend(21 + 2 * rank + 2 * (rank + 2) + 2, bcast_buff, 512));
            }
        }
        ASSERT_SYS_OK(pthread_mutex_unlock(&waitingMutex));
    }
    return NULL;
}

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();
    rank = atoi(getenv("MIMPI_rank"));
    world_size = atoi(getenv("MIMPI_world_size"));
    deadlock_detection = enable_deadlock_detection;
    deadlockDetected = false;
    performReading = true;
    sendsReceives = calloc(world_size, sizeof (int));
    ASSERT_SYS_OK(pthread_cond_init(&waiting, NULL));
    ASSERT_SYS_OK(pthread_mutex_init(&waitingMutex, NULL));
    ASSERT_SYS_OK(pthread_cond_init(&barrier, NULL));
    ASSERT_SYS_OK(pthread_mutex_init(&barrierMutex, NULL));
    head = NULL;
    pthread_t readerThread;
    ASSERT_ZERO(pthread_create(&readerThread, NULL, reader, NULL));
}

void MIMPI_Finalize() {
    performReading = false;
    free(sendsReceives);
    ASSERT_SYS_OK(pthread_cond_destroy(&waiting));
    ASSERT_SYS_OK(pthread_mutex_destroy(&waitingMutex));
    channels_finalize();
}

int MIMPI_World_size() {
    return world_size;
}

int MIMPI_World_rank() {
    return rank;
}

MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag
) {
    if(destination == rank){
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }
    if((destination >= world_size && tag != MIMPI_BCAST) || destination < 0){
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    struct Message *mess =malloc(sizeof (struct Message));
    mess->tag = tag;
    mess->source = rank;
    mess->count = count;
    void * buff = malloc(512);
    int sent = 0;
    while(sent < count) {
        int nowSent;
        memmove(buff, mess, sizeof(struct Message));
        memmove(buff + sizeof(struct Message), data + sent, 512 - sizeof(struct Message));
        ASSERT_SYS_OK(nowSent = chsend(21 + 2 * destination, buff, 512));
        sent+= max(nowSent - sizeof(struct Message), 0);
    }
    if(deadlock_detection) {
        sendsReceives[destination]++;
    }
    free(buff);
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    if(source == rank){
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }
    if((source >= world_size && tag != MIMPI_BCAST) || source < 0){
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    ASSERT_SYS_OK(pthread_mutex_lock(&waitingMutex));
    if(deadlock_detection) {
        struct Message* receivedFromYou = malloc(sizeof (struct Message));
        receivedFromYou->source = rank;
        receivedFromYou->tag = MIMPI_RECEIVED;
        receivedFromYou->count = sizeof (struct Message);
        ASSERT_SYS_OK(chsend(21 + 2 * source, receivedFromYou, sizeof (struct Message)));
        free(receivedFromYou);
    }
    struct buffElem* x = find(source, tag, count);
    while (x == NULL || x->written < x->count){
        isWaiting = true;
        waitingForSource = source;
        waitingForTag = tag;
        waitingForCount = count;
        ASSERT_SYS_OK(pthread_cond_wait(&waiting, &waitingMutex));
        isWaiting = false;
        if(deadlockDetected){
            deadlockDetected = false;
            struct Message* deadlockDetected = malloc(sizeof (struct Message));
            deadlockDetected->source = rank;
            deadlockDetected->tag = MIMPI_DEADLOCK_DETECTED;
            deadlockDetected->count = sizeof (struct Message);
            ASSERT_SYS_OK(chsend(21 + 2 * source, deadlockDetected, sizeof (struct Message)));
            free(deadlockDetected);
            sendsReceives[source]=0;
            ASSERT_SYS_OK(pthread_mutex_unlock(&waitingMutex));
            return MIMPI_ERROR_DEADLOCK_DETECTED;
        }
        x = find(source, tag, count);
    }
    memmove(data, x->data, count);
    if(x->next != NULL) {
        x->next->prev = x->prev;
    }
    if(x->prev != NULL) {
        x->prev->next = x->next;
    }
    else {
        head = x->next;
    }
    free(x->data);
    free(x);
    ASSERT_SYS_OK(pthread_mutex_unlock(&waitingMutex));
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Barrier() {
    struct Message* message = malloc(sizeof(struct Message));
    message->count = sizeof(struct Message);
    message->tag = MIMPI_BARRIER_AWAIT;
    message->source = rank;
    ASSERT_SYS_OK(pthread_mutex_lock(&barrierMutex));
    barrierUnlocked = false;
    ASSERT_SYS_OK(chsend(21 + 2*world_size, message, sizeof(struct Message)));
    while(!barrierUnlocked) {
        ASSERT_SYS_OK(pthread_cond_wait(&barrier, &barrierMutex));
    }
    ASSERT_SYS_OK(pthread_mutex_unlock(&barrierMutex));
    struct Message* ready = malloc(sizeof(struct Message));
    ready->count = sizeof(struct Message);
    ready->tag = MIMPI_BARRIER_WAKE;
    ready->source = rank;
    if(2*(rank+2) - 2 < world_size) {
        ASSERT_SYS_OK(chsend(21 + 4 * rank + 4, ready, sizeof(struct Message)));
    }
    if(2*(rank+2) - 1 < world_size) {
        ASSERT_SYS_OK(chsend(21 + 4 * rank + 6, ready, sizeof(struct Message)));
    }
    free(ready);
    free(message);
    return MIMPI_SUCCESS;
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    if(root > world_size || root < 0){
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    if(rank == root){
        MIMPI_Send(data, count, world_size, MIMPI_BCAST);
        // Broadcast message to root's children
        if(2*(rank+2) - 2 < world_size)
            MIMPI_Send(data, count, 2*(rank+2) - 2, MIMPI_BCAST);
        if(2*(rank+2) - 1 < world_size)
            MIMPI_Send(data, count, 2*(rank+2) - 1, MIMPI_BCAST);
    } else{
        MIMPI_Recv(data, count, root, MIMPI_BCAST);
    }
    return MIMPI_SUCCESS;
}

void single_reduce(u_int8_t* child_data, u_int8_t* self_data, int count, MIMPI_Op op){
    for (int i = 0; i < count; ++i) {
        switch (op) {
            case MIMPI_MAX:
                child_data[i] = child_data[i] >= self_data[i] ? child_data[i] : self_data[i];
                break;
            case MIMPI_MIN:
                child_data[i] = child_data[i] <= self_data[i] ? child_data[i] : self_data[i];
                break;
            case MIMPI_PROD:
                child_data[i] = child_data[i] * self_data[i];
                break;
            case MIMPI_SUM:
                child_data[i] = child_data[i] + self_data[i];
                break;
        }
    }
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    if(root >= world_size || root < 0){
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    u_int8_t *child_data = malloc(count+1);
    u_int8_t *self_data = malloc(count+1);
    memmove(child_data, send_data, count);
    memmove(self_data, send_data, count);
    if(world_size == 1){
        memmove(recv_data, send_data, count);
        return MIMPI_SUCCESS;
    }
    if(2*(rank+2) - 2 < world_size) {
        MIMPI_Recv(child_data, count, 2*(rank+2) - 2, MIMPI_REDUCE);
        single_reduce(child_data, self_data, count, op);
    }
    if(2*(rank+2) - 1 < world_size) {
        MIMPI_Recv(self_data, count, 2*(rank+2) - 1, MIMPI_REDUCE);
        single_reduce(child_data, self_data, count, op);
    }
    if(rank == 1){
        MIMPI_Recv(self_data, count, 0, MIMPI_REDUCE);
        single_reduce(child_data, self_data, count, op);
        if(root != 1) {
            MIMPI_Send(child_data, count, root, MIMPI_REDUCE);
        }
        else{
            memmove(recv_data, child_data, count);
        }
    }
    else if(rank == 0){
        MIMPI_Send(child_data, count, 1, MIMPI_REDUCE);
    }
    else{
        MIMPI_Send(child_data, count, rank/2 - 1, MIMPI_REDUCE);
    }
    if(rank == root && rank != 1){
        MIMPI_Recv(recv_data, count, 1, MIMPI_REDUCE);
    }
    free(child_data);
    free(self_data);
    MIMPI_Barrier();
    return MIMPI_SUCCESS;
}