#include <iostream>
#include <map>
#include <algorithm>
#include <ctime>
#include <sstream>

#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <fcntl.h>

#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include <experimental/filesystem>

using namespace std;
using namespace boost;
namespace fs = std::experimental::filesystem;

#define MAX_MESSAGE_LEN 8192
#define HTTP_NUM_BACKLOGS 20
#define ALLOWABLE_INTERVAL_SECS 4

struct IO{
    int fd; // File descriptor for this buffer
    int count; // Unread bytes in this buffer
    char *bufferPointer; // Pointer to the starting of unread portion of buffer
    char buffer[MAX_MESSAGE_LEN]; // Buffer
};

struct BOOKKEEPING_MESSAGE{
    int numberOfConnections;
    time_t timeStamp;
};

bool V_FLAG = false;
int HTTP_PORT_NO = 8000;
int httpFD;
int LOAD_BALANCER_PORT = 5000;
int heartBeatFD;

map<string, BOOKKEEPING_MESSAGE> bookKeeping;

/*
 * Gets the current time
 */
string getCurrentTime(){
    struct timeval tv;
    string time;

    gettimeofday(&tv, NULL);
    time_t currtime = tv.tv_sec;
    char buffer[31];
    strftime(buffer, 30, "%H:%M:%S.", localtime(&currtime));
    time = buffer;
    time += to_string(tv.tv_usec);

    return time;
}

/*
 * Shows a warning
 */
void warning(string message){
    if(V_FLAG){
        fprintf(stderr, "[%s]: WARNING: %s\n", getCurrentTime().c_str(), message.c_str());
    }
}

/*
 * Logs the message
 */
void log(string message){
    if(V_FLAG){
        fprintf(stdout, "[%s]: %s\n", getCurrentTime().c_str(), message.c_str());
    }
}

/*
 * Signal Handler for Ctrl-C.
 * 1. Closes all fds.
 * 2. Joins all threads.
 * 3. Closes socket fd.
 */
void signalSIGINTHandler(int signum){
    log("Closing socket fd");
    close(httpFD);
    close(heartBeatFD);
    exit(signum);
}

/*
 * Panic method, exits
 */
void panic(string message){
    fprintf(stderr, "[%s]: FATAL: %s\n", getCurrentTime().c_str(), message.c_str());
    signalSIGINTHandler(1);
}

/*
 * Initialise IO Structure
 */
void initIO(IO *io, int fd){
    io->fd = fd;
    io->count = 0;
    io->bufferPointer = io->buffer;
}

/*
 * Reads message to the buffer
 */
size_t readIO(IO *io, char *userBuffer, size_t n){
    while(io->count <= 0){
        log("Reading: Filling my buffers " + to_string(io->count));
        io->count = read(io->fd, io->buffer, sizeof(io->buffer));
        (io->buffer)[io->count] = '\0';
        log("Read: ");
        log(io->buffer);
        log("Count: " + to_string(io->count));
        if(io->count < 0){
            if(errno != EINTR){
                // interrupted by signal handler
                return -1;
            }
        }
        else if(io->count == 0){
            // EOF
            return 0;
        }
        else{
            io->bufferPointer = io->buffer;
        }
    }

    size_t cnt = n;
    if(io->count < n){
        cnt = io->count;
    }
    memcpy(userBuffer, io->bufferPointer, cnt);
    io->bufferPointer += cnt;
    io->count -= cnt;
    return cnt;
}

/*
 * Reads a line
 */
size_t readLineBufferedIO(IO *io, char *userBuffer, size_t n){
    int i, rv;
    char c, *bufferPointer = userBuffer;

    for(i = 1 ; i < n ; i ++){
        if((rv = readIO(io, &c, 1)) == 1){
            *bufferPointer++ = c;
            if(c == '\n'){
                break;
            }
        }
        else if(rv == 0){
            if(i == 1){
                return 0; // no data was read
            }
            else{
                break; // EOF, some data was read
            }
        }
        else{
            return -1; // Error
        }
    }
    *bufferPointer = 0;
    log("Buffered Reader Line");
    log("Read Line: ");
    log(userBuffer);
    log("Message Length: " + to_string(strlen(userBuffer)));
    return i;
}

/*
 * Reads n characters
 */
size_t readNBufferedIOB(IO *io, char *userBuffer, size_t n){
    size_t nLeft = n;
    size_t nRead;
    char *bufferPointer = userBuffer;

    while(nLeft > 0){
        if((nRead = readIO(io, bufferPointer, nLeft)) < 0){
            if(errno == EINTR){
                nRead = 0;
            }
            else{
                return -1;
            }
        }
        else if(nRead == 0){
            break;
        }
        nLeft -= nRead;
        bufferPointer += nRead;
    }
    return (n-nLeft);
}

size_t writeIO(int fd, char *userBuffer, size_t n){
    size_t nLeft = n;
    size_t nWritten;
    char *bufferPointer = userBuffer;

    while(nLeft > 0){
        if((nWritten = write(fd, bufferPointer, nLeft)) <= 0){
            if(errno == EINTR){
                nWritten = 0;
            }
            else{
                return -1;
            }
        }
        nLeft -= nWritten;
        bufferPointer += nWritten;
    }
    return n;
}

/*
 * Gets server with least load
 */
string getLeastLoadedServer(){
    int minValue = INT_MAX;
    string minKey = "";
    vector<string> keysForDeletion;
    log("Iterating Book:");
    for(map<string, BOOKKEEPING_MESSAGE>::iterator it = bookKeeping.begin() ; it != bookKeeping.end() ; it++){
        log(it->first + " -> " + to_string((it->second).numberOfConnections));
        time_t messageTimeStamp = (it->second).timeStamp;
        time_t now;
        time(&now);
        log("Difference in time: " + to_string(difftime(messageTimeStamp, now)));
        if(difftime(now, messageTimeStamp) >= ALLOWABLE_INTERVAL_SECS){
            log("[" + it->first + "] is too old. Getting deleted.");
            keysForDeletion.push_back(it->first);
            continue;
        }
        log("Min value yet: " + to_string(minValue));
        if(minValue > (it->second).numberOfConnections){
            log("Possible redirect to: [" + it->first + "]");
            minValue = (it->second).numberOfConnections;
            minKey = it->first;
        }
    }
    for(vector<string>::iterator it = keysForDeletion.begin() ; it != keysForDeletion.end() ; it ++){
        log("Erasing " + *it + " from Book Keeping");
        bookKeeping.erase(*it);
    }
    if(minKey.size() == 0){
        return "";
    }
    return "//" + minKey;
}

/*
 * Process HTTP Requests
 */
int processHTTPRequest(int fd){
    log("HTTP Redirecting");
    stringstream headerBuilder;
    headerBuilder << "HTTP/1.0 302 Permanently Moved\r\n";
    headerBuilder << "Location: " << getLeastLoadedServer() << "\r\n";
    headerBuilder << "Content-Length: 0\r\n";
    headerBuilder << "\r\n";
    string header = headerBuilder.str();
    log("HTTP Response: [" + header + "]");
    return writeIO(fd, const_cast<char *>(header.c_str()), header.size());
}

/*
 * Process Heartbeat
 */
int processHeartBeat(int fd){
    int noBytes;
    char buffer[MAX_MESSAGE_LEN + 1];
    struct sockaddr_in sourceAddress;
    socklen_t sourceAddressLen = sizeof sourceAddress;
    if((noBytes = recvfrom(fd, buffer, MAX_MESSAGE_LEN, 0, (struct sockaddr *)&sourceAddress, &sourceAddressLen)) == -1){
        warning("Could not receive message");
        return -1;
    }
    buffer[noBytes] = '\0';
    string message = buffer;
    log("Book Keeping [" + message + "]");
    int spaceIndex = message.find(" ");
    if(spaceIndex == string::npos){
        warning("Wrong Heart Beat Message");
        return -1;
    }
    getLeastLoadedServer();
    log("Iterating Book:");
    stringstream responseBuilder;
    for(map<string, BOOKKEEPING_MESSAGE>::iterator it = bookKeeping.begin() ; it != bookKeeping.end() ; it++){
        log(it->first + " -> " + to_string((it->second).numberOfConnections));
        int colonPos = (it->first).find(":");
        responseBuilder << (it->first).substr(colonPos + 1) << "\n";
    }
    int numberOfConnections = stoi(message.substr(0, spaceIndex));
    char *sourceIP = inet_ntoa(sourceAddress.sin_addr);
    int sourcePort = stoi(message.substr(spaceIndex + 1));
    string key = sourceIP;
    key += ":";
    key += to_string(sourcePort);
    log("Putting " + to_string(numberOfConnections) + " for server: [" + key + "]");
    time_t now;
    time(&now);
    BOOKKEEPING_MESSAGE bookKeepingMessage;
    bookKeepingMessage.numberOfConnections = numberOfConnections;
    bookKeepingMessage.timeStamp = now;
    bookKeeping[key] = bookKeepingMessage;
    if((noBytes = sendto(fd, responseBuilder.str().c_str(), responseBuilder.str().size(), 0, (struct sockaddr *)&sourceAddress, sourceAddressLen)) == -1){
        warning("Could not send heartbeat response");
        return -1;
    }
    log("Sent response: " + responseBuilder.str());
    return 0;
}

/*
 * Starts the server, accepts connections and creates threads to handle those connections.
 */
int startServer(){
    log("Creating the HTTP socket");
    httpFD = socket(PF_INET, SOCK_STREAM, 0);
    if(httpFD < 0){
        panic("Socket Error: Could not start the HTTP server");
    }
    
    struct sockaddr_in servaddr;
    memset(&servaddr, '\0', sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port = htons(HTTP_PORT_NO);
    log("Binding the HTTP socket to port " + to_string(HTTP_PORT_NO));
    if(bind(httpFD, (struct sockaddr *) &servaddr, sizeof(servaddr)) < 0){
        close(httpFD);
        panic("Bind Error: Could not bind the HTTP port");
    }
    if(listen(httpFD, HTTP_NUM_BACKLOGS) < 0){
        close(httpFD);
        panic("Listen Error: Could not listen to the HTTP port");
    }
    
    heartBeatFD = socket(PF_INET, SOCK_DGRAM, 0);
    if(heartBeatFD < 0){
        panic("Socket Error: Could not start the Heartbeat server");
    }
    memset(&servaddr, 0, sizeof servaddr);
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htons(INADDR_ANY);
    servaddr.sin_port = htons(LOAD_BALANCER_PORT);
    log("Binding the Heartbeat socket to port " + to_string(LOAD_BALANCER_PORT));
    if(bind(heartBeatFD, (struct sockaddr *)&servaddr, sizeof servaddr) != 0){
        close(heartBeatFD);
        panic("Bind Error: Could not bind the Heartbeat port");
    }

    fd_set readFD;
    int maxFD;
    int retValue;

    while(true){
        FD_ZERO(&readFD);
        maxFD = -1;
        log(to_string(httpFD));
        log(to_string(heartBeatFD));
        FD_SET(httpFD, &readFD);
        FD_SET(heartBeatFD, &readFD);
        maxFD = max(maxFD, httpFD);
        maxFD = max(maxFD, heartBeatFD);
        maxFD += 1;
        log("Listening on one of the FDs");
        retValue = select(maxFD, &readFD, NULL, NULL, NULL);
        if(retValue == -1){
            warning("Select Error: Returned -1!!! Trying again...");
        }
        else if(retValue == 0){
            warning("No FD is ready.... Still returned Something Wrong!!! Trying again...");
        }
        else{
            if(FD_ISSET(httpFD, &readFD)){
                struct sockaddr_in theirAddr;
                socklen_t theirAddrLen = sizeof theirAddr;
                int connFD = accept(httpFD, (sockaddr *)&theirAddr, &theirAddrLen);
                processHTTPRequest(connFD);
            }
            else if(FD_ISSET(heartBeatFD, &readFD)){
                processHeartBeat(heartBeatFD);
            }
        }
    }
}

int main(int argc, char *argv[]){    
    int c;
    char *val, *temp;
    while((c = getopt(argc, argv, "h:l:av")) != -1){
        switch(c){
            case 'h': 
                val = optarg;
                HTTP_PORT_NO = strtol(val, &temp, 0);
                if(*temp == '\0'){
                    if(HTTP_PORT_NO <= 0){
                        panic("HTTP Port Number can not be less than 1.");
                    }
                }
                else{
                    panic("HTTP Port Number is not an integer.");
                }
                break;
            case 'l': 
                val = optarg;
                LOAD_BALANCER_PORT = strtol(val, &temp, 0);
                if(*temp == '\0'){
                    if(LOAD_BALANCER_PORT <= 0){
                        panic("Load Balancer Port Number can not be less than 1.");
                    }
                }
                else{
                    panic("Load Balancer Port Number is not an integer.");
                }
                break;
            case 'a':
                panic("Devanshu Jain (devjain)");
            case 'v':
                V_FLAG = true;
                break;
            case '?':
                if(optopt == 'p'){
                    stringstream msgBuilder;
                    msgBuilder << "Option " << optopt << " requires an argument.";
                    panic(msgBuilder.str());
                }
                else{
                    stringstream msgBuilder;
                    msgBuilder << "Unknown option `" << optopt << "`";
                    panic(msgBuilder.str());
                }
            default:
                abort();
        }
    }
    log("Registering SIGINT handler");
    
    // SIGINT registered
    if(signal(SIGINT, signalSIGINTHandler) == SIG_ERR){
        panic("Signal Error: Could not register signal handler");
    }

    startServer();

    return 0;
}