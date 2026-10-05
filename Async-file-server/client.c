#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define PORT 8080
#define BUFFER_SIZE 2048

struct Request
{
    char operation[10];
    char filename[256];
    char data[1024];
};

int main()
{
    int socketFD;

    struct sockaddr_in serverAddress;
    struct Request request;

    char response[BUFFER_SIZE];

    /* Create socket */
    socketFD = socket(AF_INET, SOCK_STREAM, 0);

    if (socketFD < 0)
    {
        perror("socket");
        return 1;
    }

    memset(&serverAddress, 0, sizeof(serverAddress));

    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(PORT);

    inet_pton(AF_INET,
              "127.0.0.1",
              &serverAddress.sin_addr);

    /* Connect to server */
    if (connect(socketFD,
                (struct sockaddr *)&serverAddress,
                sizeof(serverAddress)) < 0)
    {
        perror("connect");
        close(socketFD);
        return 1;
    }

    memset(&request, 0, sizeof(request));

    printf("\n====================================\n");
    printf("      FILE PROCESSING CLIENT\n");
    printf("====================================\n");

    /* Get operation */
    printf("Enter operation (READ/WRITE): ");
    scanf("%9s", request.operation);

    /* Get filename */
    printf("Enter filename: ");
    scanf("%255s", request.filename);

    /* Get data for WRITE */
    if (strcmp(request.operation, "WRITE") == 0)
    {
        printf("Enter data: ");

        getchar();

        fgets(request.data,
              sizeof(request.data),
              stdin);

        request.data[strcspn(request.data, "\n")] = '\0';
    }

    /* Send request */
    send(socketFD,
         &request,
         sizeof(request),
         0);

    /* Receive result */
    memset(response, 0, sizeof(response));

    ssize_t n = recv(socketFD,
                     response,
                     sizeof(response) - 1,
                     0);

    printf("\n");

    if (n > 0)
    {
        response[n] = '\0';

        printf("[SERVER RESPONSE] %s\n", response);
    }
    else
    {
        printf("[SERVER RESPONSE] No response from server.\n");
    }

    printf("\n====================================\n\n");

    close(socketFD);

    return 0;
}