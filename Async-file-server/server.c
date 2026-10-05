#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/select.h>
#include <semaphore.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>

#define PORT 8080
#define BUFFER_SIZE 2048
#define SEM_NAME "/async_file_server_stats"

struct Request
{
    char operation[10];
    char filename[256];
    char data[1024];
};

/* Shared across all worker processes so throughput can be
   calculated across the whole server, not just one worker. */
typedef struct
{
    int completedCount;
    struct timeval serverStart;
} SharedStats;

SharedStats *stats;
sem_t *statsLock;

double timeDiffMs(struct timeval start, struct timeval end)
{
    return (end.tv_sec - start.tv_sec) * 1000.0 +
           (end.tv_usec - start.tv_usec) / 1000.0;
}

void processRequest(int client, struct Request *req,
                     struct timeval requestTime, int pipeWriteFd)
{
    char response[BUFFER_SIZE];
    char logMsg[BUFFER_SIZE];

    memset(response, 0, sizeof(response));

    printf("[WORKER %d] Processing %s request\n",
           getpid(), req->operation);

    /* WRITE operation */
    if (strcasecmp(req->operation, "WRITE") == 0)
    {
        strcpy(req->operation, "WRITE");
        FILE *fp = fopen(req->filename, "w");

        if (fp == NULL)
        {
            snprintf(response, sizeof(response),
                     "WRITE failed: cannot open %s",
                     req->filename);
        }
        else
        {
            fprintf(fp, "%s\n", req->data);
            fclose(fp);

            snprintf(response, sizeof(response),
                     "WRITE completed: data saved to %s",
                     req->filename);
        }
    }

    /* READ operation */
    else if (strcasecmp(req->operation, "READ") == 0)
    {
        strcpy(req->operation, "READ");
        FILE *fp = fopen(req->filename, "r");

        if (fp == NULL)
        {
            snprintf(response, sizeof(response),
                     "READ failed: cannot open %s",
                     req->filename);
        }
        else
        {
            /* Leave room for the latency text appended below */
            size_t n = fread(response, 1,
                             sizeof(response) - 200, fp);

            response[n] = '\0';

            fclose(fp);
        }
    }

    /* Invalid operation */
    else
    {
        snprintf(response, sizeof(response),
                 "Invalid operation. Use READ or WRITE.");
    }

    /* ---- Completion time + latency ---- */
    struct timeval completionTime;
    gettimeofday(&completionTime, NULL);
    double latencyMs = timeDiffMs(requestTime, completionTime);

    /* ---- Update the shared throughput counter ----
       Several worker processes can finish at nearly the same time,
       so the increment is protected by a semaphore. */
    sem_wait(statsLock);
    stats->completedCount++;
    int totalCompleted = stats->completedCount;
    sem_post(statsLock);

    double elapsedSec = timeDiffMs(stats->serverStart, completionTime) / 1000.0;
    double throughput = (elapsedSec > 0) ? totalCompleted / elapsedSec : 0.0;

    /* Append latency to the reply sent back to the client */
    size_t used = strlen(response);
    snprintf(response + used, sizeof(response) - used,
             " | latency=%.2fms", latencyMs);

    send(client, response, strlen(response), 0);

    /* ---- Report this completion to the parent through the pipe ----
       This is our completion queue: every finished request is written
       here so the parent can log it and update throughput. */
    snprintf(logMsg, sizeof(logMsg),
             "[COMPLETION QUEUE] worker=%d op=%s file=%s latency=%.2fms total=%d throughput=%.2f req/s\n",
             getpid(), req->operation, req->filename,
             latencyMs, totalCompleted, throughput);

    write(pipeWriteFd, logMsg, strlen(logMsg));

    printf("[COMPLETE] Result returned by worker %d (latency %.2f ms)\n\n",
           getpid(), latencyMs);
}

void removeFinishedChildren()
{
    int status;

    while (waitpid(-1, &status, WNOHANG) > 0)
    {
    }
}

int main()
{
    /* Flush stdout after every newline instead of only when the buffer
       fills up. Without this, text printed before fork() (like the
       startup banner) can sit in the buffer and get printed again by
       a child process later -- especially when output is redirected
       to a file instead of a live terminal. */
    setvbuf(stdout, NULL, _IOLBF, 0);

    int serverSocket;
    int clientSocket;

    struct sockaddr_in serverAddress;
    struct sockaddr_in clientAddress;

    socklen_t clientLength = sizeof(clientAddress);

    struct Request request;

    int pipefd[2];

    /* ---- Shared memory for the throughput counter ---- */
    stats = mmap(NULL, sizeof(SharedStats), PROT_READ | PROT_WRITE,
                 MAP_SHARED | MAP_ANONYMOUS, -1, 0);

    if (stats == MAP_FAILED)
    {
        perror("mmap");
        return 1;
    }

    /* A named semaphore is used instead of sem_init() because sem_init()
       is not reliably supported for locks shared between processes on
       macOS. sem_open() works the same way on macOS and Linux. */
    sem_unlink(SEM_NAME); /* clear any leftover semaphore from a previous run */

    statsLock = sem_open(SEM_NAME, O_CREAT, 0644, 1);

    if (statsLock == SEM_FAILED)
    {
        perror("sem_open");
        munmap(stats, sizeof(SharedStats));
        return 1;
    }

    stats->completedCount = 0;
    gettimeofday(&stats->serverStart, NULL);

    /* ---- Pipe used by every worker to report completions ---- */
    if (pipe(pipefd) < 0)
    {
        perror("pipe");
        return 1;
    }

    /* Create socket */
    serverSocket = socket(AF_INET, SOCK_STREAM, 0);

    if (serverSocket < 0)
    {
        perror("socket");
        return 1;
    }

    int option = 1;

    setsockopt(serverSocket,
               SOL_SOCKET,
               SO_REUSEADDR,
               &option,
               sizeof(option));

    /* Server address */
    memset(&serverAddress, 0,
           sizeof(serverAddress));

    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    serverAddress.sin_port = htons(PORT);

    /* Bind */
    if (bind(serverSocket,
             (struct sockaddr *)&serverAddress,
             sizeof(serverAddress)) < 0)
    {
        perror("bind");
        close(serverSocket);
        return 1;
    }

    /* Listen */
    if (listen(serverSocket, 10) < 0)
    {
        perror("listen");
        close(serverSocket);
        return 1;
    }

    printf("\n====================================\n");
    printf(" ASYNCHRONOUS FILE PROCESSING SERVER\n");
    printf("====================================\n");
    printf("[SERVER] Listening on port %d...\n\n",
           PORT);

    while (1)
    {
        /* Remove finished child processes */
        removeFinishedChildren();

        /* Wait for either a new connection or a completion report,
           so the server never blocks only on accept(). */
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(serverSocket, &readfds);
        FD_SET(pipefd[0], &readfds);

        int maxfd = (serverSocket > pipefd[0]) ? serverSocket : pipefd[0];

        int ready = select(maxfd + 1, &readfds, NULL, NULL, NULL);

        if (ready < 0)
        {
            if (errno == EINTR)
                continue;

            perror("select");
            continue;
        }

        /* A worker reported a completed request */
        if (FD_ISSET(pipefd[0], &readfds))
        {
            char buf[BUFFER_SIZE];

            ssize_t n = read(pipefd[0], buf, sizeof(buf) - 1);

            if (n > 0)
            {
                buf[n] = '\0';
                printf("%s", buf);
            }
        }

        /* A new client is waiting to connect */
        if (FD_ISSET(serverSocket, &readfds))
        {
            clientSocket = accept(serverSocket,
                                  (struct sockaddr *)&clientAddress,
                                  &clientLength);

            if (clientSocket < 0)
            {
                if (errno == EINTR)
                    continue;

                perror("accept");
                continue;
            }

            printf("----------------------------------------\n");
            printf("[CONNECT] Client connected from %s\n",
                   inet_ntoa(clientAddress.sin_addr));

            /* Request arrival time -> start of latency measurement */
            struct timeval requestTime;
            gettimeofday(&requestTime, NULL);

            /* Receive request */
            memset(&request, 0, sizeof(request));

            ssize_t received = recv(clientSocket,
                                    &request,
                                    sizeof(request),
                                    0);

            if (received <= 0)
            {
                close(clientSocket);
                continue;
            }

            /* Guarantee null-termination across buffers */
            request.operation[sizeof(request.operation) - 1] = '\0';
            request.filename[sizeof(request.filename) - 1] = '\0';
            request.data[sizeof(request.data) - 1] = '\0';

            printf("[QUEUE] Request received\n");

            /* Create child worker */
            pid_t pid = fork();

            if (pid < 0)
            {
                perror("fork");
                close(clientSocket);
                continue;
            }

            /* Child process = Worker */
            if (pid == 0)
            {
                close(serverSocket);
                close(pipefd[0]); /* worker only writes to the pipe */

                processRequest(clientSocket, &request, requestTime, pipefd[1]);

                close(clientSocket);
                exit(0);
            }

            /* Parent process = Main Server */
            close(clientSocket);

            printf("[SERVER] Worker %d created.\n", pid);
            printf("[SERVER] Ready for another request.\n\n");
        }
    }

    close(serverSocket);
    sem_close(statsLock);
    sem_unlink(SEM_NAME);

    return 0;
}