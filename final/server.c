#include "segel.h"
#include "request.h"
#include "log.h"



typedef struct{
    int connfd;
    struct timeval arrivalTime;
} job_t;

typedef struct{
    job_t** jobsArray;
    int head;
    int tail;
    int pendingRequest;
    int capacity;

} queue_t;

typedef struct{
    queue_t* queue;
    int* workingRequests;
    server_log log;
    threads_stats t;
} thread_args;


pthread_mutex_t mutex;
pthread_cond_t isEmpty;
pthread_cond_t isFull;

//
// server.c: A very, very simple web server
//
// To run:
//  ./server <tcp_portnum> <udp_portnum> <threads> <queue_size> <debug_sleep_time>
//
// Repeatedly handles HTTP requests sent to tcp_portnum, and thread-statistics
// pings sent to udp_portnum. Most of the work is done within routines written
// in request.c
//

// Parses and validates command-line arguments. Every invalid argument
// results in an informative message on stderr and an immediate exit(1).
void getargs(int *tcpPort, int *udpPort, int *threads, int *queueSize, double *sleepTime, int argc, char *argv[])
{
    if (argc != 6) {
        fprintf(stderr, "Usage: %s <tcp_portnum> <udp_portnum> <threads> <queue_size> <debug_sleep_time>\n", argv[0]);
        exit(1);
    }

    *tcpPort = atoi(argv[1]);
    *udpPort = atoi(argv[2]);
    *threads = atoi(argv[3]);
    *queueSize = atoi(argv[4]);
    *sleepTime = atof(argv[5]);

    if (*tcpPort <= 1024 || *tcpPort > 65535) {
        fprintf(stderr, "Invalid tcp_portnum: must be above 1024 and at most 65535\n");
        exit(1);
    }

    if (*udpPort <= 1024 || *udpPort > 65535) {
        fprintf(stderr, "Invalid udp_portnum: must be above 1024 and at most 65535\n");
        exit(1);
    }

    if (*udpPort == *tcpPort) {
        fprintf(stderr, "Invalid udp_portnum: must be different than tcp_portnum\n");
        exit(1);
    }

    if (*threads <= 0) {
        fprintf(stderr, "Invalid threads: must be a positive integer\n");
        exit(1);
    }

    if (*queueSize <= 0) {
        fprintf(stderr, "Invalid queue_size: must be a positive integer\n");
        exit(1);
    }
}

void* workerThread(void* arg) {
    thread_args* args = (thread_args*)arg;

    queue_t* q = args->queue;
    int* wr = args->workingRequests;
    server_log log =  args->log;
    threads_stats stats_t = (threads_stats)args->t;
    free(arg);

    while(1){
        pthread_mutex_lock(&mutex);
        while(q->pendingRequest == 0){
            pthread_cond_wait(&isEmpty, &mutex);
        }
        job_t* job = q->jobsArray[q->head];
        q->head = (q->head + 1) % q->capacity;
        q->pendingRequest--;

        (*wr)++;
        struct timeval dispatch_time;

        gettimeofday(&dispatch_time, NULL);

        pthread_mutex_unlock(&mutex);

        time_stats ts;
        ts.task_dispatch = dispatch_time;
        ts.task_arrival = job->arrivalTime;

        requestHandle(job->connfd, ts, stats_t, log);

        pthread_mutex_lock(&mutex);
        Close(job->connfd);
        free(job);
        (*wr)--;
        pthread_cond_signal(&isFull);
        pthread_mutex_unlock(&mutex);
    }
    pthread_exit(NULL);
}

// Handles a single UDP statistics ping: the datagram's payload is the
// decimal thread id (1..threadsCount) whose stats should be returned.
// Invalid/unrecognized ids are silently ignored (not a fatal server error).
void handleUdpPing(int udpfd, threads_stats* statsArray, int threadsCount)
{
    char buf[MAXLINE];
    struct sockaddr_in clientaddr;

    int n = UDP_Read(udpfd, &clientaddr, buf, MAXLINE - 1);
    if (n < 0) {
        fprintf(stderr, "UDP_Read failed: %s\n", strerror(errno));
        return;
    }
    buf[n] = '\0';

    int id = atoi(buf);
    if (id < 1 || id > threadsCount) {
        return;
    }

    char resp[MAXBUF];
    int len = format_thread_stats(statsArray[id - 1], resp);
    UDP_Write(udpfd, &clientaddr, resp, len);
}

int main(int argc, char *argv[])
{
    // Create the global server log
    int workingRequest = 0;

    pthread_mutex_init(&mutex, NULL);
    pthread_cond_init(&isEmpty, NULL);
    pthread_cond_init(&isFull, NULL);

    int listenfd, udpfd, connfd, tcpPort, udpPort, clientlen, threadsCount, queueSize;
    double sleepTime;
    struct sockaddr_in clientaddr;

    getargs(&tcpPort, &udpPort, &threadsCount, &queueSize, &sleepTime, argc, argv);
    server_log request_log = create_log(sleepTime);

    queue_t queue;


    pthread_t* workerHandles = malloc(sizeof(pthread_t) * threadsCount);

    if (workerHandles == NULL){
        fprintf(stderr, "malloc failed");
        exit(1);
    }

    threads_stats* statsArray = malloc(sizeof(threads_stats) * threadsCount);
    if (statsArray == NULL){
        fprintf(stderr, "malloc failed");
        exit(1);
    }

    queue.capacity = queueSize;
    queue.jobsArray= malloc(sizeof(job_t*)*queueSize);
    queue.head = 0;
    queue.tail = 0;
    queue.pendingRequest = 0;

    for (unsigned int i = 0; i < threadsCount; i++){
        thread_args* args = malloc(sizeof(thread_args));
        if (args == NULL){
            fprintf(stderr, "malloc failed");
            exit(1);
        }
        threads_stats stats = (threads_stats)malloc(sizeof(struct Threads_stats));
        if(stats == NULL){
            fprintf(stderr, "malloc failed");
            exit(1);
        }
        stats->id = i + 1;             // Thread ID, distributed 1..N
        stats->stat_req = 0;       // Static request count
        stats->dynm_req = 0;       // Dynamic request count
        stats->post_req = 0;        // Post request count
        stats->total_req = 0; // Total request count
        pthread_mutex_init(&stats->lock, NULL);

        statsArray[i] = stats;

        args->queue = &queue;
        args->workingRequests = &workingRequest;
        args->log = request_log;
        args->t = stats;
        pthread_create(&workerHandles[i], NULL, workerThread, (void*)args);
    }

    listenfd = Open_listenfd(tcpPort);
    udpfd = UDP_Open(udpPort);

    int maxfd = (listenfd > udpfd ? listenfd : udpfd) + 1;

    while (1) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(listenfd, &readfds);
        FD_SET(udpfd, &readfds);

        if (Select(maxfd, &readfds, NULL, NULL, NULL) < 0) {
            continue;
        }

        // UDP pings are handled first, ahead of dispatching TCP jobs.
        if (FD_ISSET(udpfd, &readfds)) {
            handleUdpPing(udpfd, statsArray, threadsCount);
        }

        if (FD_ISSET(listenfd, &readfds)) {
            clientlen = sizeof(clientaddr);
            connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t*) &clientlen);

            struct timeval arrival;
            gettimeofday(&arrival, NULL);

            pthread_mutex_lock(&mutex);

            while((workingRequest + queue.pendingRequest) >= queue.capacity){
                pthread_cond_wait(&isFull, &mutex);
            }

            job_t* new_job = malloc(sizeof(job_t));
            if (new_job == NULL){
                fprintf(stderr, "malloc failed");
                exit(1);
            }
            queue.jobsArray[queue.tail] = new_job;
            new_job->connfd = connfd;
            new_job->arrivalTime= arrival;
            queue.tail = (queue.tail + 1) % queue.capacity;
            queue.pendingRequest++;

            pthread_cond_signal(&isEmpty);
            pthread_mutex_unlock(&mutex);
        }
    }

    // Clean up the server log before exiting
    while (queue.pendingRequest){
        job_t* job = queue.jobsArray[queue.head];
        queue.head = (queue.head + 1) % queue.capacity;
        queue.pendingRequest--;
        free(job);
    }
    free(queue.jobsArray);
    destroy_log(request_log);

    for (int i = 0; i < threadsCount; i++) {
        pthread_join(workerHandles[i], NULL);
    }
    free(workerHandles);

    for (int i = 0; i < threadsCount; i++) {
        pthread_mutex_destroy(&statsArray[i]->lock);
        free(statsArray[i]);
    }
    free(statsArray);

    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&isEmpty);
    pthread_cond_destroy(&isFull);
}
