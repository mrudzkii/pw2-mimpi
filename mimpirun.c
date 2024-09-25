/**
 * This file is for implementation of mimpirun program.
 * */

#include "mimpi_common.h"
#include "channel.h"
//#include "mimpi.h"
#include <assert.h>
#include <stdlib.h>
#include<unistd.h>
#include <sys/wait.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>

bool work = false;
int world_size;

void* helper(){
    struct Message * ready = malloc(sizeof (struct Message));
    ready->tag = MIMPI_BARRIER_WAKE;
    ready->count = sizeof (struct Message);
    ready->source = world_size;
    struct Message* message = malloc(sizeof (struct Message));
    void* bcast_buff = malloc(512);
    int onBarrier = 0;

    while (work){
        ASSERT_SYS_OK(chrecv(20 + 2*world_size, message, sizeof (struct Message)));
        if(message->tag == MIMPI_BARRIER_AWAIT){
            onBarrier++;
            if(onBarrier == world_size){
                if(world_size >= 1) ASSERT_SYS_OK(chsend(21, ready, sizeof (struct Message)));
                if(world_size >= 2) ASSERT_SYS_OK(chsend(23, ready, sizeof (struct Message)));
                onBarrier = 0;
            }
        }
        else if(message->tag == MIMPI_BCAST){
            memmove(bcast_buff, message, sizeof (struct Message));
            ASSERT_SYS_OK(chrecv(20 + 2*world_size, bcast_buff + sizeof (struct Message), 512));
            if(world_size >= 1 && message->source != 0) ASSERT_SYS_OK(chsend(21, bcast_buff, 512));
            if(world_size >= 2 && message->source != 1) ASSERT_SYS_OK(chsend(23, bcast_buff, 512));
        }

    }
    free(ready);
    free(bcast_buff);
    return NULL;
}

int main(int argc, char* argv[]) {
    assert(argc >= 3);
    int n = atoi(argv[1]);
    world_size = n;
    char* prog = argv[2];
    char* args[argc-1];
    char* name = strrchr(prog, '/');
    args[0] = name == NULL ? prog : name + 1;
    int i = 3;
    while (argv[i] != NULL){
        args[i-2] = argv[i];
        i++;
    }
    args[i-2] = NULL;
    char number[4];
    int ret = snprintf(number, sizeof(number), "%d", n);
    if (ret < 0 || ret >= (int)sizeof(number)) {
        fatal("Error in snprintf.");
    }
    ASSERT_ZERO(setenv("MIMPI_world_size", number, 1));

    channels_init();
    work = true;

    for (int j = 0; j < n+1; ++j) {   // helper process has descriptors 20+2*n and 21+2*n
        int chn_dsc[2];
        ASSERT_SYS_OK(channel(chn_dsc));
        ASSERT_SYS_OK(dup2(chn_dsc[0], 20 + 2*j));
        close(chn_dsc[0]);
        ASSERT_SYS_OK(dup2(chn_dsc[1], 21 + 2*j));
        close(chn_dsc[1]);
//        printf("created: %d, %d\n", 20+2*j, 21+2*j);
    }
    pthread_t helper_thread;
    ASSERT_ZERO(pthread_create(&helper_thread, NULL, helper, NULL));

    for (int i = 0; i < n; ++i) {
        pid_t pid = fork();
        char number[4];
        int ret = snprintf(number, sizeof(number), "%d", i);
        if (ret < 0 || ret >= (int)sizeof(number))
            fatal("Error in snprintf.");
        if(pid == 0) { //child process
            ASSERT_ZERO(setenv("MIMPI_rank", number, 1));
            ASSERT_SYS_OK(execvp(prog, args));
        }
    }
    for (int i = 0; i < n; ++i) {
        ASSERT_SYS_OK(wait(NULL));
    }
    for (int i = 0; i < n+1; ++i) {
        ASSERT_SYS_OK(close(20+2*i));
        ASSERT_SYS_OK(close(21+2*i));
    }
    work = false;
    return 0;
}