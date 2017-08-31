#include <iostream>
#include <map>
#include <algorithm>
#include <ctime>
#include <sstream>
#include <fstream>
#include <chrono>

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

#include "base64.h"

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/regex.hpp>
#include <boost/algorithm/string/regex.hpp>

#include "json.h"

#include <experimental/filesystem>

#include "kvs_client.cc"
#include "MailService.h"

using namespace std;
using namespace boost;
namespace fs = std::experimental::filesystem;

#define MAX_MESSAGE_LEN 8192
#define SESSION_VALID_HOUR 2
#define SESSION_VALID_MIN 0
#define SESSION_VALID_SEC 0
#define SESSION_ID_LEN 1000
#define HEARTBEAT_INTERVAL_SECS 2

struct HTTPRequest{
    string method;
    string uri;
    string version;
    map<string, string> headers;
    string body;
};

struct HTTPResponse{
    string statusLine;
    map<string, string> headers;
    string body;
};

struct IO{
    int fd; // File descriptor for this buffer
    int count; // Unread bytes in this buffer
    char *bufferPointer; // Pointer to the starting of unread portion of buffer
    char buffer[MAX_MESSAGE_LEN]; // Buffer
};

struct Cookie{
    string username;
    string validUntil;
};

map<int, pthread_t> allThreads; // Mapping fd to thread id
map<string, Cookie> sessionBook; // maintaining cookies
int sockfd; // socket fd
bool DEEP_V_FLAG = false;
bool V_FLAG = false;
int PORT_NO = 8001;
string LOAD_BALANCER = "127.0.0.1";
string BACKEND_SERVER = "127.0.0.1:50051";
int LOAD_BALANCER_PORT = 5000;
string CGI_DIR = "cgi-bin";
string WWW_DIR = "www";
string ROOT_DIR = ".";

KVSClientInterface bigTableClient;

vector<string> httpServersInAction;

/*
 * Gets the time after hr hours, min minutes and sec seconds
 */
string getTimeFromNow(int hr, int min, int sec){
    chrono::time_point<std::chrono::system_clock> timePoint;
    const char* fmt = "%a, %d %b %Y %T %Z";
    char outstr[200];
    struct tm *tmp;

    timePoint = chrono::system_clock::now();
    timePoint = timePoint + chrono::hours(hr);
    timePoint = timePoint + chrono::minutes(min);
    timePoint = timePoint + chrono::seconds(sec);

    time_t adjustedTimePoint = chrono::system_clock::to_time_t(timePoint);
    tmp = gmtime(&adjustedTimePoint);
    strftime(outstr, sizeof(outstr), fmt, tmp);

    string time(outstr);

    return time;
}

/*
 * gets chrono timepoint from string
 */
chrono::system_clock::time_point getTimeForString(string time){
    chrono::time_point<std::chrono::system_clock> timePoint;

    const char* fmt = "%a, %d %b %Y %T %Z";
    struct tm tmp;
    strptime(time.c_str(), fmt, &tmp);
    time_t t = mktime(&tmp);
    timePoint = chrono::system_clock::from_time_t(t);

    return timePoint;
}

/*
 * Gets current time
 */
string getCurrentTime(){
    return getTimeFromNow(0,0,0);
}

/*
 * Shows a warning
 */
void warning(string message){
    if(V_FLAG){
        fprintf(stderr, "[%s]: WARNING: %s\n", getCurrentTime().c_str(), message.c_str());
        fflush(stderr);
    }
}

/*
 * Logs the message
 */
void log(string message){
    if(V_FLAG){
        fprintf(stdout, "[%s]: %s\n", getCurrentTime().c_str(), message.c_str());
        fflush(stdout);
    }
}

/*
 * Deep Logs the message
 */
void dlog(string message){
    if(DEEP_V_FLAG){
        fprintf(stdout, "[%s]: %s\n", getCurrentTime().c_str(), message.c_str());
        fflush(stdout);
    }
}

/*
 * Signal Handler for Ctrl-C.
 * 1. Closes all fds.
 * 2. Joins all threads.
 * 3. Closes socket fd.
 */
void signalSIGINTHandler(int signum){
    string msg = "-ERR Server shutting down\n";
    for(map<int, pthread_t>::iterator mapIterator = allThreads.begin() ; mapIterator != allThreads.end() ; mapIterator ++){
        send(mapIterator->first, msg.c_str(), strlen(msg.c_str()), 0);
        log("[" + to_string(mapIterator->first) + "] Connection Closed");
        close(mapIterator->first);
    }
    for(map<int, pthread_t>::iterator mapIterator = allThreads.begin() ; mapIterator != allThreads.end() ; mapIterator ++){
        log("Joining Thread");
        pthread_cancel(mapIterator->second);
        pthread_join(mapIterator->second, NULL);
    }
    log("Closing socket fd");
    close(sockfd);
    exit(signum);
}

void sendHeartBeat(int signum){
    log("BEEP!!!");
    int noBytes;
    int fd = socket(PF_INET, SOCK_DGRAM, 0);
    string message = to_string(allThreads.size()) + " " + to_string(PORT_NO);
    sockaddr_in destination;
    memset(&destination, 0, sizeof destination);
    destination.sin_family = AF_INET;
    destination.sin_port = htons(LOAD_BALANCER_PORT);
    inet_pton(AF_INET, LOAD_BALANCER.c_str(), &(destination.sin_addr));
    if((noBytes = sendto(fd, message.c_str(), message.size(), 0, (struct sockaddr *)&destination, sizeof destination)) == -1){
        warning("Could not send heartbeat");
    }
    char buffer[MAX_MESSAGE_LEN + 1];
    struct sockaddr_in sourceAddress;
    socklen_t sourceAddressLen = sizeof sourceAddress;
    if((noBytes = recvfrom(fd, buffer, MAX_MESSAGE_LEN, 0, (struct sockaddr *)&sourceAddress, &sourceAddressLen)) == -1){
        warning("Could not receive!!!");
    }
    buffer[noBytes] = '\0';
    string response = buffer;
    split(httpServersInAction, response, is_any_of("\n"));
    httpServersInAction.pop_back();
    log("Servers in action: " + to_string(httpServersInAction.size()));
    for(vector<string>::iterator it = httpServersInAction.begin() ; it != httpServersInAction.end() ; it ++){
        log(*it);
    }
    alarm(HEARTBEAT_INTERVAL_SECS);
}

/*
 * Panic method, exits
 */
void panic(string message){
    fprintf(stderr, "[%s]: FATAL: %s\n", getCurrentTime().c_str(), message.c_str());
    fflush(stderr);
    signalSIGINTHandler(1);
}

/*
 * Prints HTTP Request Headers
 */
void printGetHeaders(HTTPRequest *requestMessage){
    cout << "Current State of headers" << endl;
    cout << requestMessage->method + " " + requestMessage->uri + " " + requestMessage->version << endl;
    map<string, string> headers = requestMessage->headers;
    for(map<string, string>::iterator it = headers.begin() ; it != headers.end() ; it ++){
        cout << it->first + ": " + it->second << endl;
    }
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
        dlog("Reading: Filling my buffers " + to_string(io->count));
        io->count = read(io->fd, io->buffer, sizeof(io->buffer));
        (io->buffer)[io->count] = '\0';
        dlog("Read: ");
        dlog(io->buffer);
        dlog("Count: " + to_string(io->count));
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

    for(i = 1 ; i <= n ; i ++){
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
    dlog("Buffered Reader Line");
    dlog("Read Line: ");
    dlog(userBuffer);
    dlog("Message Length: " + to_string(strlen(userBuffer)));
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
        log("writing 1");
        if((nWritten = write(fd, bufferPointer, nLeft)) <= 0){
            log("less than 0 ???");
            if(errno == EINTR){
                nWritten = 0;
            }
            else{
                return -1;
            }
        }
        log("writing 2");
        nLeft -= nWritten;
        bufferPointer += nWritten;
    }
    log("done");
    return n;
}

string readFile(string fileName){
    ifstream file(fileName, ios::in);
    string data;
    file.seekg(0, ios::end);
    int size = file.tellg();
    data.resize(size);
    file.seekg(0, ios::beg);
    file.read(&data[0], data.size());
    file.close();
    return data;
}

void writeFile(string fileName, string content){
    ofstream file(fileName.c_str(), ios::out | ios::binary);
    file.write(content.c_str(), content.size());
    file.close();
}

/*
 * Gets file type
 */
string getFileType(string fileName){
    if(ends_with(fileName, ".html")){
        return "text/html";
    }
    else if(ends_with(fileName, ".js")){
        return "application/javascript";
    }
    else if(ends_with(fileName, ".css")){
        return "text/css";
    }
    else if(ends_with(fileName, ".json")){
        return "application/json";
    }
    else if(ends_with(fileName, ".png")){
        return "image/png";
    }
    else if(ends_with(fileName, ".jpg") || ends_with(fileName, ".jpeg")){
        return "image/jpeg";
    }
    else if(ends_with(fileName, ".gif")){
        return "image/gif";
    }
    else{
        return "text/plain";
    }
}

/*
 * Checks if the filename corresponds to correct cgi file
 */
bool isCorrectDynamicRequestFile(string fileName){
    if(strcmp(fileName.c_str(), "/cgi-bin/login") == 0){

    }
    else if(strcmp(fileName.c_str(), "/cgi-bin/logout") == 0){

    }
    else if(strcmp(fileName.c_str(), "/cgi-bin/signup") == 0){
        
    }
    else if(strcmp(fileName.c_str(), "/cgi-bin/getMail") == 0){
        
    }
    else if(strcmp(fileName.c_str(), "/cgi-bin/sendMail") == 0){
        
    }
    else if(strcmp(fileName.c_str(), "/cgi-bin/getFile") == 0){
        
    }
    else if(strcmp(fileName.c_str(), "/cgi-bin/sendFile") == 0){
        
    }
    else if(strcmp(fileName.c_str(), "/cgi-bin/shareFile") == 0){
        
    }
    else if(strcmp(fileName.c_str(), "/cgi-bin/createFolder") == 0){
        
    }
    else if(strcmp(fileName.c_str(), "/cgi-bin/admin") == 0){

    }
    else if(strcmp(fileName.c_str(), "/cgi-bin/startServer") == 0){

    }
    else if(strcmp(fileName.c_str(), "/cgi-bin/killServer") == 0){

    }
    else if(strcmp(fileName.c_str(), "/cgi-bin/deleteFolder") == 0){

    }
    else if(strcmp(fileName.c_str(), "/cgi-bin/renameFile") == 0){

    }
    else if(strcmp(fileName.c_str(), "/cgi-bin/moveFile") == 0){

    }
    else if(strcmp(fileName.c_str(), "/cgi-bin/deleteFile") == 0){

    }
    else{
        return false;
    }
    return true;
}

/*
 * Builds and sends client error
 */
void clientError(int fd, string cause, int errorCode, string shortMessage, string LongMessage){
    stringstream bodyBuilder, headerBuilder;

    bodyBuilder << "<html><title>Server Error</title></html>\r\n";
    bodyBuilder << "<body bgcolor='ffffff'>\r\n";
    bodyBuilder << errorCode << " : " << shortMessage << "\r\n";
    bodyBuilder << "<p>" << LongMessage << " : " << cause << "</p>";
    bodyBuilder << "</body>";

    string body = bodyBuilder.str();

    headerBuilder << "HTTP/1.0 " << errorCode << " " << shortMessage << "\r\n";
    headerBuilder << "Content-Type: text/html\r\n";
    headerBuilder << "Content-Length: " << body.size() << "\r\n\r\n";

    string header = headerBuilder.str();
    log("Response:\n" + header + body);

    writeIO(fd, const_cast<char *>((header + body).c_str()), (header + body).size());
}

/*
 * Redirects user to location
 */
void redirect(IO *io, string location){
    stringstream headerBuilder;
    headerBuilder << "HTTP/1.0 302 Permanently Moved\r\n";
    headerBuilder << "Location: " << location << "\r\n";
    headerBuilder << "Content-Length: 0\r\n";
    headerBuilder << "\r\n";
    string header = headerBuilder.str();
    log("HTTP Response: [" + header + "]");
    writeIO(io->fd, const_cast<char *>(header.c_str()), header.size());
}

/*
 * Build response to be sent
 */
void sendResponse(IO *io, map<string, string> headers, string body, HTTPRequest *requestMessage){
    stringstream headerBuilder;
    headerBuilder << "HTTP/1.0 200 OK\r\n";
    if(headers.find("Content-Type") != headers.end()){
        headerBuilder << "Content-Type: " << headers["Content-Type"] << "\r\n";
        headers.erase("Content-Type");
    }
    else{
        headerBuilder << "Content-Type: text/html\r\n";
    }
    if(headers.find("Content-Length") != headers.end()){
        headerBuilder << "Content-Length: " << headers["Content-Length"] << "\r\n";
        headers.erase("Content-Length");
    }
    else{
        headerBuilder << "Content-Length: " << body.size() << "\r\n";
    }
    for(map<string, string>::iterator it = headers.begin() ; it != headers.end() ; it ++){
        headerBuilder << it->first << ": " << it->second << "\r\n";
    }
    headerBuilder << "\r\n";

    string header = headerBuilder.str();
    log("Response:\n" + header + body);

    if(strcasecmp((requestMessage->method).c_str(), "HEAD") == 0){
        writeIO(io->fd, const_cast<char *>(header.c_str()), header.size());
    }
    else{
        writeIO(io->fd, const_cast<char *>((header + body).c_str()), (header + body).size());
    }
}

/*
 * Build response to be sent
 */
void sendResponse(IO *io, map<string, string> headers, vector<BYTE> body, HTTPRequest *requestMessage){
    stringstream headerBuilder;
    headerBuilder << "HTTP/1.0 200 OK\r\n";
    if(headers.find("Content-Type") != headers.end()){
        headerBuilder << "Content-Type: " << headers["Content-Type"] << "\r\n";
        headers.erase("Content-Type");
    }
    else{
        headerBuilder << "Content-Type: text/html\r\n";
    }
    if(headers.find("Content-Length") != headers.end()){
        headerBuilder << "Content-Length: " << headers["Content-Length"] << "\r\n";
        headers.erase("Content-Length");
    }
    else{
        headerBuilder << "Content-Length: " << body.size() << "\r\n";
    }
    for(map<string, string>::iterator it = headers.begin() ; it != headers.end() ; it ++){
        headerBuilder << it->first << ": " << it->second << "\r\n";
    }
    headerBuilder << "\r\n";

    string header = headerBuilder.str();

    writeIO(io->fd, const_cast<char *>(header.c_str()), header.size());

    if(strcasecmp((requestMessage->method).c_str(), "HEAD") != 0){
        for(int i = 0 ; i < body.size() ; i ++){
            char buf[1] = {body[i]};
            write(io->fd, buf, 1);
        }
    }
}

/*
 * Generates Session ID for a username
 */
string generateSessionId(int length){
    char alphaNum[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    stringstream sessionId;
    srand (time(NULL));
    for(int i = 0 ; i < length ; i ++){
        sessionId << alphaNum[rand() % strlen(alphaNum)];
    }
    return sessionId.str();
}

bool isLoggedIn(HTTPRequest *requestMessage, string &sessionId){
    map<string, string> requestHeaders = requestMessage->headers;
    if(requestHeaders.find("cookie") == requestHeaders.end()){
        return false;
    }
    else{
        string cookie = requestHeaders["cookie"];
        vector<string> cookieComponents;
        split(cookieComponents, cookie, is_any_of(";"));
        for(int i = 0 ; i < cookieComponents.size() ; i++){
            cookieComponents[i] = trim_copy(cookieComponents[i]);
            int equalPos = cookieComponents[i].find("=");
            if(equalPos == string::npos){
                continue;
            }
            else{
                string key = cookieComponents[i].substr(0, equalPos);
                string value = cookieComponents[i].substr(equalPos + 1);
                if(strcasecmp(key.c_str(), "sid") == 0){
                    if(sessionBook.find(value) == sessionBook.end()){
                        return false;
                    }
                    else{
                        Cookie sessionCookie = sessionBook[value];
                        chrono::system_clock::time_point validUntil = getTimeForString(sessionCookie.validUntil);
                        chrono::system_clock::time_point currentTime = getTimeForString(getCurrentTime());
                        chrono::milliseconds difference = chrono::duration_cast<chrono::milliseconds>(validUntil - currentTime);
                        if(difference.count() <= 0){
                            sessionBook.erase(value);
                            return false;
                        }
                        else{
                            sessionId = value;
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}

/*
 * Checks if row,col is present in big table 
 */
bool doesExists(string row, string column, string &value){
    log("Accessing >" + row + "<,>" + column + "< of big table");
    value = bigTableClient.get(row, column);
    log("Value is: >" + value + "<");

    if(strcasecmp(value.c_str(), "RPC failed") == 0){
        return false;
    }
    else if(strcasecmp(value.c_str(), "0") == 0){
        return false;
    }
    else{
        return true;
    }
}

/*
 * Parses multipart/form-data
 */
// int parseMultipartFormDataBackUp(IO *io, string boundary, HTTPRequest *requestMessage, map<string, string> &multipart_text, map<string, string> &multipart_file){
//     char bodyBuffer[stoi(requestMessage->headers["content-length"]) + 1];
//     readNBufferedIOB(io, bodyBuffer, stoi(requestMessage->headers["content-length"]));
//     bodyBuffer[stoi(requestMessage->headers["content-length"])] = '\0';
//     fprintf(stdout, "\n\n\n\n\n>>%x<<\n\n\n\n\n", bodyBuffer);
//     string bodyContent = bodyBuffer;
//     log("Body Content:\n>>>" + bodyContent + "<<<\n\n");
//     vector<string> bodyParts;
//     split_regex(bodyParts, bodyContent, regex("--" + boundary));
//     if(bodyParts.size() == 0){
//         return -1;
//     }
//     log("Body parts: " + to_string(bodyParts.size()));
//     for(int i = 1 ; i < bodyParts.size() - 1 ; i ++){
//         log("body part: " + to_string(i));
//         string part = trim_copy(bodyParts[i]);
//         vector<string> headers;
//         int CRLFPos = part.find("\r\n");
//         string line = part.substr(0, CRLFPos);
//         while(line.size() != 0){
//             headers.push_back(line);
//             part = part.substr(CRLFPos + 2);
//             CRLFPos = part.find("\r\n");
//             line = part.substr(0, CRLFPos);
//         }
//         part = part.substr(CRLFPos + 2);
//         log("HEADERS: \n" + algorithm::join(headers, "\n"));
//         log("Content: \n>" + part + "<\n\n");
//         writeFile(to_string(i), part);
//         log("Written");
//     }
//     multipart_text["abc"] = "def";
//     multipart_file["123"] = "456";
//     return 0;
// }

/*
 * Parses multipart/form-data
 * 
 */
int parseMultipartFormData(IO *io, string boundary, HTTPRequest *requestMessage, map<string, string> &multipart_text, map<string, string> &multipart_file){
    stringstream last_boundary;
    last_boundary << "--" << boundary << "--"; 
    string end = last_boundary.str();  
    char userBuffer[stoi(requestMessage->headers["content-length"]) + 1];
    int i=0;
    readLineBufferedIO(io, userBuffer, stoi(requestMessage->headers["content-length"])); 
    string str(userBuffer);
    while(str.find(end) == string::npos) {
        if(str.find("filename") != string::npos){
            string file_content = "";
            string file = str.substr(str.find("filename"));
            size_t start_pos = file.find('"');
            size_t last_pos = file.find_last_of('"');
            string filename = file.substr(start_pos+1, last_pos-start_pos-1);
            readLineBufferedIO(io, userBuffer, stoi(requestMessage->headers["content-length"]));  //checking content-type
            str = userBuffer; 
            // cout << str << endl;
            if(str.find("application/octet-stream") == string::npos){ //checking if a file has been uploaded
                // i = i+1;
                while(str.find(boundary) == string::npos ){ //reading the file body
                    if(str.find("Content-Type") == string::npos){
                        // cout << str << endl;
                        file_content +=str;
                    }
                    readLineBufferedIO(io, userBuffer, stoi(requestMessage->headers["content-length"])); 
                    str = userBuffer;   
                    // cout << str << endl;                 
                }
                multipart_file.insert ( std::pair<string, string>(trim_copy(filename), trim_copy(file_content)));
            }
        }
        else if(str.find("name") != string::npos && strlen(str.c_str()) > 2){
            string name = str.substr(str.find("name"));
            size_t start_pos = name.find('"');
            size_t last_pos = name.find_last_of('"');
            string textname = name.substr(start_pos+1, last_pos-start_pos-1);
            string text_content = "";
            while(str.find(boundary) == string::npos){
                if(str.find("Content-Disposition") == string::npos){
                 /*   str = trim_copy(str);*/
                    text_content +=str;
                }
                readLineBufferedIO(io, userBuffer, stoi(requestMessage->headers["content-length"])); 
                str = userBuffer;  
            }
            multipart_text.insert ( std::pair< string, string>(trim_copy(textname), trim_copy(text_content)));
        }
        if (str.find(end) != string::npos ){
            break;
        }
        readLineBufferedIO(io, userBuffer, stoi(requestMessage->headers["content-length"])); 
        str = userBuffer;           
    }

    for(auto it = multipart_text.begin() ; it != multipart_text.end() ; it ++){
        cout << it->first << ":" << it->second << endl;
    }
    for(auto it = multipart_file.begin() ; it != multipart_file.end() ; it ++){
        cout << it->first << ":" << it->second << endl;
    }
    return 0;
}

/*
 * Checks if uri is accessible or not. 
 * Also REPLACES uri: "/" to appropriate "/dashboard.html", if logged in or "/index.html" if not
 */
bool isPageAccessible(HTTPRequest *requestMessage){
    string uri = requestMessage->uri;
    if(strcasecmp(uri.c_str(), "/") == 0){
        string sessionId;
        if(isLoggedIn(requestMessage, sessionId)){
            requestMessage->uri = "/dashboard.html";
        }
        else{
            requestMessage->uri = "/index.html";
        }
        return true;
    }
    else if(strcmp(uri.c_str(), "/dashboard.html") == 0){
        string sessionId;
        if(isLoggedIn(requestMessage, sessionId)){
            return true;
        }
        else{
            return false;
        }
    }
    else if(strcmp(uri.c_str(), "/cgi-bin/getMail") == 0){
        string sessionId;
        if(isLoggedIn(requestMessage, sessionId)){
            return true;
        }
        else{
            return false;
        }
    }
    else if(strcmp(uri.c_str(), "/cgi-bin/sendMail") == 0){
        string sessionId;
        if(isLoggedIn(requestMessage, sessionId)){
            return true;
        }
        else{
            return false;
        }
    }
    else if(strcmp(uri.c_str(), "/cgi-bin/getFile") == 0){
        string sessionId;
        if(isLoggedIn(requestMessage, sessionId)){
            return true;
        }
        else{
            return false;
        }
    }
    else if(strcmp(uri.c_str(), "/cgi-bin/sendFile") == 0){
        string sessionId;
        if(isLoggedIn(requestMessage, sessionId)){
            return true;
        }
        else{
            return false;
        }
    }
    else if(strcmp(uri.c_str(), "/cgi-bin/createFolder") == 0){
        string sessionId;
        if(isLoggedIn(requestMessage, sessionId)){
            return true;
        }
        else{
            return false;
        }
    }
    else if(strcmp(uri.c_str(), "/cgi-bin/shareFile") == 0){
        string sessionId;
        if(isLoggedIn(requestMessage, sessionId)){
            return true;
        }
        else{
            return false;
        }
    }
    else if(strcmp(uri.c_str(), "/cgi-bin/deleteFolder") == 0){
        string sessionId;
        if(isLoggedIn(requestMessage, sessionId)){
            return true;
        }
        else{
            return false;
        }
    }
    else if(strcmp(uri.c_str(), "/cgi-bin/moveFile") == 0){
        string sessionId;
        if(isLoggedIn(requestMessage, sessionId)){
            return true;
        }
        else{
            return false;
        }
    }
    else if(strcmp(uri.c_str(), "/cgi-bin/renameFile") == 0){
        string sessionId;
        if(isLoggedIn(requestMessage, sessionId)){
            return true;
        }
        else{
            return false;
        }
    }
    else if(strcmp(uri.c_str(), "/cgi-bin/deleteFile") == 0){
        string sessionId;
        if(isLoggedIn(requestMessage, sessionId)){
            return true;
        }
        else{
            return false;
        }
    }

    return true;
}

/*
 * deletes folder recursively
 */
int deleteFile(IO *io, string user, string source, string path){
    string contentStr;
    string sourceContentStr;
    Json::Value sourceContent;
    Json::FastWriter writer;
    Json::Reader reader;
    
    if(!doesExists(user, source, sourceContentStr)){
        clientError(io->fd, source, 400, "invalid folder", "no such entry in big table");
        return -1;
    }
    if(!reader.parse(sourceContentStr.c_str(), sourceContent)){
        clientError(io->fd, sourceContentStr, 500, "json error", "Content is not stored in json format");
        return -1;
    }
    if(!sourceContent["type"].asBool()){
        clientError(io->fd, sourceContentStr, 500, "Its a file", "Trying to delete something from file . Wrong request. Should not happen!!!");
        return -1;
    }
    if(!doesExists(user, path, contentStr)){
        clientError(io->fd, path, 400, "invalid folder", "no such entry in big table");
        return -1;
    }
    log("Deleting : >" + user + "< >" + path + "<");
    string response = bigTableClient.del(user, path);
    Json::Value sourceFolderSubFiles = sourceContent["files"];
    sourceFolderSubFiles.removeMember(path);
    sourceContent["files"] = sourceFolderSubFiles;
    sourceContentStr = writer.write(sourceContent);
    log("putting " + sourceContentStr + " in " + user + "," + source);
    bigTableClient.put(user, source, sourceContentStr);
    return 0;
}

/*
 * deletes folder recursively
 */
int deleteFolder(IO *io, string user, string source, string path){
    Json::FastWriter writer;
    Json::Reader reader;
    string folderContentStr;
    Json::Value folderContent;
    string sourceContentStr;
    Json::Value sourceContent;
    
    if(!doesExists(user, source, sourceContentStr)){
        clientError(io->fd, source, 400, "invalid folder", "no such entry in big table");
        return -1;
    }
    if(!reader.parse(sourceContentStr.c_str(), sourceContent)){
        clientError(io->fd, sourceContentStr, 500, "json error", "Content is not stored in json format");
        return -1;
    }
    if(!sourceContent["type"].asBool()){
        clientError(io->fd, sourceContentStr, 500, "Its a file", "Trying to delete something from file . Wrong request. Should not happen!!!");
        return -1;
    }
    if(!doesExists(user, path, folderContentStr)){
        clientError(io->fd, path, 400, "invalid folder", "no such entry in big table");
        return -1;
    }
    if(!reader.parse(folderContentStr.c_str(), folderContent)){
        clientError(io->fd, folderContentStr, 500, "json error", "Content is not stored in json format");
        return -1;
    }
    if(!folderContent["type"].asBool()){
        clientError(io->fd, folderContentStr, 500, "Its a file", "Trying to delete file . Wrong request. Should not happen!!!");
        return -1;
    }
    Json::Value subFolders = folderContent["folders"];
    Json::Value subFiles = folderContent["files"];
    for(Json::ValueIterator it = subFolders.begin() ; it != subFolders.end() ; it ++){
        log("Deleting folder: " + (*it).asString() + " in " + path);
        deleteFolder(io, user, path, it.key().asString());
    }
    for(Json::ValueIterator it = subFiles.begin() ; it != subFiles.end() ; it ++){
        log("Deleting file: " + (*it).asString() + " in " + path);
        deleteFile(io, user, path, it.key().asString());
    }
    log("Deleting : >" + user + "< >" + path + "<");
    bigTableClient.del(user, path);
    Json::Value sourceFolderSubFolders = sourceContent["folders"];
    sourceFolderSubFolders.removeMember(path);
    sourceContent["folders"] = sourceFolderSubFolders;
    sourceContentStr = writer.write(sourceContent);
    log("putting " + sourceContentStr + " in " + user + "," + source);
    bigTableClient.put(user, source, sourceContentStr);
    return 0;
}

/*
 * CGI for login
 */
int CGI_LOGIN(IO *io, map<string, string> CGIArguments, HTTPRequest *requestMessage){
    string username = CGIArguments["username"];
    string password = CGIArguments["password"];
    string response = bigTableClient.get(username, "password");

    string body = "";
    map<string, string> headers;

    if(strcasecmp(response.c_str(), password.c_str()) == 0){
        string sessionId = generateSessionId(SESSION_ID_LEN);
        while(sessionBook.find(sessionId) != sessionBook.end()){
            sessionId = generateSessionId(SESSION_ID_LEN);
        }
        body = "Succesfully logged in";
        string validUntil = getTimeFromNow(SESSION_VALID_HOUR, SESSION_VALID_MIN, SESSION_VALID_SEC);
        Cookie cookie;
        cookie.username = username;
        cookie.validUntil = validUntil;
        sessionBook[sessionId] = cookie;
        headers["Set-Cookie"] = "sid=" + sessionId + "; Expires=" + validUntil + "; Path=/";
    }
    else if(strcasecmp(response.c_str(), "RPC failed") == 0){
        body = "RPC failed";
    }
    else{
        body = "Wrong credentials";
    }
    sendResponse(io, headers, body, requestMessage);
    return 0;
}

/*
 * CGI for logout
 */
int CGI_LOGOUT(IO *io, map<string, string> CGIArguments, HTTPRequest *requestMessage){
    string sessionId;
    if(!isLoggedIn(requestMessage, sessionId)){
        log("user: is not currently logged in");
        sendResponse(io, map<string, string>(), "You are not logged in!", requestMessage);
    }
    else{
        log("user: " + sessionBook[sessionId].username + " is currently logged in");
        sessionBook.erase(sessionId);
        sendResponse(io, map<string, string>(), "You are succesfully logged out!", requestMessage);
    }
    return 0;
}

/*
 * CGI for signup
 */
int CGI_SIGNUP(IO *io, map<string, string> CGIArguments, HTTPRequest *requestMessage){
    string username = CGIArguments["username"];
    string password = CGIArguments["password"];
    string response = bigTableClient.put(username, "password", password);
    if(strcasecmp(response.c_str(), "RPC failed") == 0){
        sendResponse(io, map<string, string>(), "RPC failed while storing password", requestMessage);
        return 0;
    }

    // INIT
    Json::Value root;
    Json::Value folder;
    Json::Value file;

    Json::Value inbox(Json::arrayValue);
    Json::Value sent(Json::arrayValue);

    Json::FastWriter writer;
    
    root["type"] = true;
    root["folders"] = folder;
    root["files"] = file;

    string folderContent = writer.write(root);
    log("initial file content: \n" + folderContent);
    response = bigTableClient.put(username, "root", folderContent);
    if(strcasecmp(response.c_str(), "RPC failed") == 0){
        sendResponse(io, map<string, string>(), "RPC failed while storing default files", requestMessage);
        return -1;
    }

    folderContent = writer.write(inbox);
    log("initial inbox content: \n" + folderContent);
    response = bigTableClient.put(username, "inbox", folderContent);
    if(strcasecmp(response.c_str(), "RPC failed") == 0){
        sendResponse(io, map<string, string>(), "RPC failed while making inbox folder", requestMessage);
        return -1;
    }

    folderContent = writer.write(sent);
    log("initial sent content: \n" + folderContent);
    response = bigTableClient.put(username, "sent", folderContent);
    if(strcasecmp(response.c_str(), "RPC failed") == 0){
        sendResponse(io, map<string, string>(), "RPC failed while making sent folder", requestMessage);
        return -1;
    }
    sendResponse(io, map<string, string>(), "Succesfully signed up!", requestMessage);
    return 0;
}

/*
 * CGI for getMail
 */
int CGI_GET_MAIL(IO *io, map<string, string> CGIArguments, HTTPRequest *requestMessage){
    if(CGIArguments.find("path") == CGIArguments.end()){
        stringstream response;
        response << "<html>" << "\r\n";
        response << "<body>" << "\r\n";
        response << "<form action=\"/cgi-bin/getMail\" method=\"get\">" << "\r\n";
        response << "<input type=\"hidden\" name=\"path\" value=\"inbox\">" << "\r\n";
        response << "<button type=\"submit\">" << "Inbox" << "</button>" << "\r\n";
        response << "</form>" << "\r\n";
        response << "<form action=\"/cgi-bin/getMail\" method=\"get\">" << "\r\n";
        response << "<input type=\"hidden\" name=\"path\" value=\"sent\">" << "\r\n";
        response << "<button type=\"submit\">" << "Sent" << "</button>" << "\r\n";
        response << "</form>" << "\r\n";
        response << "</body>" << "\r\n";
        response << "</html>" << "\r\n";
        sendResponse(io, map<string, string>(), response.str(), requestMessage);
        return 0;
    }
    string sessionId;
    if(!isLoggedIn(requestMessage, sessionId)){
        log("user is not currently logged in");
        sendResponse(io, map<string, string>(), "You are not logged in!", requestMessage);
        return -1;
    }
    string user = sessionBook[sessionId].username;
    string path = CGIArguments["path"];
    Json::Reader reader;
    if(strcasecmp(path.c_str(), "inbox") == 0){
        log("inbox");
        stringstream response;
        response << "<html>" << "\r\n";
        response << "<body>" << "\r\n";
        string inboxContentStr = bigTableClient.get(user, "inbox");
        log(inboxContentStr);
        if(strcasecmp(inboxContentStr.c_str(), "RPC failed") == 0){
            clientError(io->fd, inboxContentStr, 400, "no such path", "There is no such path (inbox) for this user: " + user);
            return -1;
        }
        Json::Value inboxContent;
        if(!reader.parse(inboxContentStr.c_str(), inboxContent)){
            clientError(io->fd, inboxContentStr, 500, "json error", "Content is not stored in json format");
            return -1;
        }
        for(Json::ValueIterator it = inboxContent.begin() ; it != inboxContent.end() ; it ++){
            string key = (*it).asString();
            string mailContentStr = bigTableClient.get(user, key);
            if(strcasecmp(mailContentStr.c_str(), "RPC failed") == 0){
                clientError(io->fd, mailContentStr, 400, "no such path", "There is no such path " + key + " for this user: " + user);
                return -1;
            }
            Json::Value mailContent;
            if(!reader.parse(mailContentStr.c_str(), mailContent)){
                clientError(io->fd, mailContentStr, 500, "json error", "Content is not stored in json format");
                return -1;
            }
            response << "From: " << mailContent["from"].asString() << "<br>\r\n";
            response << "To: " << mailContent["to"].asString() << "<br>\r\n";
            response << "Date: " << mailContent["date"].asString() << "<br>\r\n";
            response << "Subject: " << mailContent["subject"].asString() << "<br>\r\n";
            response << "Message: " << mailContent["message"].asString() << "<br>\r\n";
            response << "Attachments: " << "<br>\r\n";
            Json::Value attachments = mailContent["attachment"];
            log("getting attachments");
            for(Json::ValueIterator attachIterator = attachments.begin() ; attachIterator != attachments.end() ; attachIterator ++){
                string attachedFileKey = (*attachIterator).asString();
                log("attached file key: " + attachedFileKey);
                string attachedContentStr = bigTableClient.get(user, attachedFileKey);
                if(strcasecmp(attachedContentStr.c_str(), "RPC failed") == 0){
                    clientError(io->fd, attachedContentStr, 400, "no such path", "There is no such path " + attachedFileKey + " for this user: " + user);
                    return -1;
                }
                Json::Value attachedContent;
                if(!reader.parse(attachedContentStr.c_str(), attachedContent)){
                    clientError(io->fd, attachedContentStr, 500, "json error", "Content is not stored in json format");
                    return -1;
                }
                response << "<form action=\"/cgi-bin/getFile\" method=\"get\">" << "\r\n";
                response << "<input type=hidden name=\"path\" value=\"" << attachedFileKey << "\">" << "\r\n";
                response << "<button type=\"submit\">" << attachedContent["filename"].asString() << "</button>" << "\r\n";
                response << "</form><br>" << "\r\n";
            }
            log("done getting attachments");
            response << "<br><hr><br>\r\n";
        }
        response << "</body>" << "\r\n";
        response << "</html>" << "\r\n";
        sendResponse(io, map<string, string>(), response.str(), requestMessage);
        return 0;
    }
    else if(strcasecmp(path.c_str(), "sent") == 0){
        log("sent");
        stringstream response;
        response << "<html>" << "\r\n";
        response << "<body>" << "\r\n";
        string inboxContentStr = bigTableClient.get(user, "sent");
        log(inboxContentStr);
        if(strcasecmp(inboxContentStr.c_str(), "RPC failed") == 0){
            clientError(io->fd, inboxContentStr, 400, "no such path", "There is no such path (inbox) for this user: " + user);
            return -1;
        }
        Json::Value inboxContent;
        if(!reader.parse(inboxContentStr.c_str(), inboxContent)){
            clientError(io->fd, inboxContentStr, 500, "json error", "Content is not stored in json format");
            return -1;
        }
        for(Json::ValueIterator it = inboxContent.begin() ; it != inboxContent.end() ; it ++){
            string key = (*it).asString();
            string mailContentStr = bigTableClient.get(user, key);
            if(strcasecmp(mailContentStr.c_str(), "RPC failed") == 0){
                clientError(io->fd, mailContentStr, 400, "no such path", "There is no such path " + key + " for this user: " + user);
                return -1;
            }
            Json::Value mailContent;
            if(!reader.parse(mailContentStr.c_str(), mailContent)){
                clientError(io->fd, mailContentStr, 500, "json error", "Content is not stored in json format");
                return -1;
            }
            response << "From: " << mailContent["from"].asString() << "<br>\r\n";
            response << "To: " << mailContent["to"].asString() << "<br>\r\n";
            response << "Date: " << mailContent["date"].asString() << "<br>\r\n";
            response << "Subject: " << mailContent["subject"].asString() << "<br>\r\n";
            response << "Message: " << mailContent["message"].asString() << "<br>\r\n";
            response << "Attachments: " << "<br>\r\n";
            Json::Value attachments = mailContent["attachment"];
            log("getting attachments");
            for(Json::ValueIterator attachIterator = attachments.begin() ; attachIterator != attachments.end() ; attachIterator ++){
                string attachedFileKey = (*attachIterator).asString();
                log("attached file key: " + attachedFileKey);
                string attachedContentStr = bigTableClient.get(user, attachedFileKey);
                if(strcasecmp(attachedContentStr.c_str(), "RPC failed") == 0){
                    clientError(io->fd, attachedContentStr, 400, "no such path", "There is no such path " + attachedFileKey + " for this user: " + user);
                    return -1;
                }
                Json::Value attachedContent;
                if(!reader.parse(attachedContentStr.c_str(), attachedContent)){
                    clientError(io->fd, attachedContentStr, 500, "json error", "Content is not stored in json format");
                    return -1;
                }
                response << "<form action=\"/cgi-bin/getFile\" method=\"get\">" << "\r\n";
                response << "<input type=hidden name=\"path\" value=\"" << attachedFileKey << "\">" << "\r\n";
                response << "<button type=\"submit\">" << attachedContent["filename"].asString() << "</button>" << "\r\n";
                response << "</form><br>" << "\r\n";
            }
            log("done getting attachments");
            response << "<br><hr><br>\r\n";
        }
        response << "</body>" << "\r\n";
        response << "</html>" << "\r\n";
        sendResponse(io, map<string, string>(), response.str(), requestMessage);
        return 0;
    }
    else{
        clientError(io->fd, path, 400, "wrong path", "The specified path is wrong for emails");
        return -1;
    }
}

/*
 * CGI for sendMail
 */
int CGI_SEND_MAIL(IO *io, string boundary, HTTPRequest *requestMessage){
    map<string, string> multipartText;
    map<string, string> multipartFile;
    parseMultipartFormData(io, boundary, requestMessage, multipartText, multipartFile);

    vector<string> recipients;
    string sender;
    string subject;
    string message;
    stringstream responseBuilder;

    string sessionId;
    if(!isLoggedIn(requestMessage, sessionId)){
        log("user is not currently logged in");
        sendResponse(io, map<string, string>(), "You are not logged in!", requestMessage);
        return -1;
    }
    sender = sessionBook[sessionId].username;

    if(multipartText.find("rcptTo") == multipartText.end()){
        clientError(io->fd, "rcptTo", 400, "Recipient missing", "There is no recipient in CGI arguments");
        return -1;
    }
    else{
        string rcpt = multipartText["rcptTo"];
        if(trim_copy(rcpt).size() == 0){
            clientError(io->fd, "Recipient: >" + rcpt + "<", 400, "Recipient missing", "Recipient is empty string in CGI arguments");
            return -1;
        }
        else{
            rcpt = trim_copy(rcpt);
            split(recipients, rcpt, is_any_of(","));
            for(int i = 0 ; i < recipients.size() ; i ++){
                recipients[i] = trim_copy(recipients[i]);
            }
        }
    }

    if(multipartText.find("subject") == multipartText.end()){
        clientError(io->fd, "subject", 400, "Subject missing", "There is no subject in CGI arguments");
        return -1;
    }
    else{
        subject = trim_copy(multipartText["subject"]);
    }

    if(multipartText.find("message") == multipartText.end()){
        clientError(io->fd, "message", 400, "Message missing", "There is no message in CGI arguments");
        return -1;
    }
    else{
        message = trim_copy(multipartText["message"]);
    }

    bool is_forward = false;
    for(vector<string>::iterator it = recipients.begin() ; it != recipients.end() ; it ++){
        string recipient = *it;
        int atPos = recipient.find("@");
        string domain = recipient.substr(atPos + 1);
        if(atPos == string::npos){
            clientError(io->fd, "Recipient: <" + recipient + ">", 400, "Recipient ill-formed", "Position of '@' not found in recippient.");
            return -1;
        }

        if(strcasecmp(domain.c_str(), "localhost.com") == 0){
            string rcptUser = recipient.substr(0, atPos);
            string pswd;
            if(doesExists(rcptUser, "password", pswd)){

            }
            else{
                clientError(io->fd, "Recipient: <" + recipient + ">", 400, "Recipient ill-formed", "User does not exist in localhost db.");
                return -1;
            }
        }
        else{
            is_forward = true;
        }
    }

    vector<string> dataBodyParts;
    string hash;
    if(is_forward){
        stringstream dataBodyBuilder;
        dataBodyBuilder << "Content-Type: text/plain" << "\r\n\r\n";
        dataBodyBuilder << message;
        dataBodyParts.push_back(dataBodyBuilder.str());

        for(map<string, string>::iterator it = multipartFile.begin() ; it != multipartFile.end() ; it ++){
            dataBodyBuilder.str("");
            dataBodyBuilder.clear();
            dataBodyBuilder << "Content-Type: application/octet-stream; name=\"" << it->first << "\"\r\n";
            dataBodyBuilder << "Content-Description: " << it->first << "\r\n";
            dataBodyBuilder << "Content-Disposition: attachment; filename=\"" << it->first << "\"\r\n";
            dataBodyBuilder << "Content-Transfer-Encoding: BASE64" << "\r\n\r\n";
            dataBodyBuilder << trim_copy(it->second) << "\r\n\r\n";
            dataBodyParts.push_back(dataBodyBuilder.str());
        }

        hash = "----==" + generateSessionId(20) + "==";
        string dataBody = algorithm::join(dataBodyParts, "\n");
        while(dataBody.find(hash) != string::npos){
            hash = "----==" + generateSessionId(20) + "==";
        }
    }

    Json::FastWriter writer;
    Json::Reader reader;
    string currentTime = getCurrentTime();
            
    for(vector<string>::iterator it = recipients.begin() ; it != recipients.end() ; it ++){
        log("Trying to send mail to " + (*it));
        string recipient = *it;
        int atPos = recipient.find("@");
        string domain = recipient.substr(atPos + 1);
        if(strcasecmp(domain.c_str(), "localhost.com") == 0){
            string rcptUser = recipient.substr(0, atPos);
            Json::Value mailContent;
            mailContent["to"] = algorithm::join(recipients, ",");
            mailContent["from"] = sender + "@localhost.com";
            mailContent["subject"] = subject;
            mailContent["date"] = currentTime;
            mailContent["message"] = message;
            log("message is: " + mailContent["message"].asString());
            Json::Value attachments(Json::arrayValue);
            int indexAttachment = 0;
            for(map<string, string>::iterator it = multipartFile.begin() ; it != multipartFile.end() ; it ++){
                string key = sender + "-" + rcptUser + "-" + to_string(time(NULL)) + "-attachment-" + to_string(indexAttachment);
                indexAttachment ++;
                Json::Value attachedFile;
                attachedFile["filename"] = it->first;
                attachedFile["content"] = it->second;
                attachedFile["type"] = false;
                string value = writer.write(attachedFile);
                string response = bigTableClient.put(rcptUser, key, value);
                if(strcasecmp(response.c_str(), "RPC failed") == 0){
                    clientError(io->fd, "Response: >" + response + "<", 500, "RPC Error", "RPC has failed.");
                    return -1;
                }
                attachments.append(key);
            }
            log("adding attachments");
            mailContent["attachment"] = attachments;
            log("done adding attachments");

            string mailContentStr = writer.write(mailContent);
            string mailContentKeyStr = to_string(time(NULL));
            string response = bigTableClient.put(rcptUser, mailContentKeyStr, mailContentStr);
            if(strcasecmp(response.c_str(), "RPC failed") == 0){
                clientError(io->fd, "Response: >" + response + "<", 500, "RPC Error", "RPC has failed.");
                return -1;
            }
            string inboxContentStr = bigTableClient.get(rcptUser, "inbox");
            if(strcasecmp(inboxContentStr.c_str(), "RPC failed") == 0){
                clientError(io->fd, inboxContentStr, 400, "no such path", "There is no such path (inbox) for this user: " + recipient);
                return -1;
            }
            log(inboxContentStr);
            Json::Value inboxContent;
            if(!reader.parse(inboxContentStr.c_str(), inboxContent)){
                clientError(io->fd, inboxContentStr, 500, "json error", "Content is not stored in json format");
                return -1;
            }
            log("appending key to inbox folder");
            inboxContent.append(mailContentKeyStr);
            log("done appending to inbox folder");
            inboxContentStr = writer.write(inboxContent);
            response = bigTableClient.put(rcptUser, "inbox", inboxContentStr);
            if(strcasecmp(response.c_str(), "RPC failed") == 0){
                clientError(io->fd, "Response: >" + response + "<", 500, "RPC Error", "RPC has failed.");
                return -1;
            }
            log("sent successfully");
            responseBuilder << "SUCCESS Added mail to " << rcptUser << "'s inbox" << "\r\n";
        }
        else{
            stringstream dataBuilder;
            dataBuilder << "Date: " << currentTime << "\r\n";
            dataBuilder << "To: " << algorithm::join(recipients, ",") << "\r\n";
            dataBuilder << "From: " << sender << "@localhost.com" << "\r\n";
            dataBuilder << "Subject: " << subject << "\r\n";
            dataBuilder << "MIME-Version: 1.0" << "\r\n";
            dataBuilder << "Content-Type: multipart/mixed; boundary=\"" << hash << "\"\r\n\r\n";
            for(vector<string>::iterator it = dataBodyParts.begin() ; it != dataBodyParts.end() ; it ++){
                dataBuilder << "--" << hash << "\r\n";
                dataBuilder << *it << "\r\n";
            }
            dataBuilder << "--" << hash << "--" << "\r\n\r\n.\r\n";
            log("Sending: " + dataBuilder.str());
            MailService mailService(sender + "@localhost.com", recipient, dataBuilder.str());
            int response = mailService.sendMail();
            if(response == 0){
                responseBuilder << "SUCCESS sending email to " << recipient << "\r\n";
            }
            else{
                responseBuilder << "ERROR sending email to " << recipient << "\r\n";
            }
        }
    }

    Json::Value mailContent;
    mailContent["to"] = algorithm::join(recipients, ",");
    mailContent["from"] = sender + "@localhost.com";
    mailContent["subject"] = subject;
    mailContent["date"] = currentTime;
    mailContent["message"] = message;
    Json::Value attachments(Json::arrayValue);
    int indexAttachment = 0;
    for(map<string, string>::iterator it = multipartFile.begin() ; it != multipartFile.end() ; it ++){
        string key = sender + "-" + to_string(time(NULL)) + "-attachment-" + to_string(indexAttachment);
        indexAttachment ++;
        Json::Value attachedFile;
        attachedFile["filename"] = it->first;
        attachedFile["content"] = it->second;
        attachedFile["type"] = false;
        string value = writer.write(attachedFile);
        string response = bigTableClient.put(sender, key, value);
        if(strcasecmp(response.c_str(), "RPC failed") == 0){
            clientError(io->fd, "Response: >" + response + "<", 500, "RPC Error", "RPC has failed.");
            return -1;
        }
        attachments.append(key);
    }
    mailContent["attachment"] = attachments;

    string mailContentStr = writer.write(mailContent);
    string mailContentKeyStr = to_string(time(NULL));
    string response = bigTableClient.put(sender, mailContentKeyStr, mailContentStr);
    if(strcasecmp(response.c_str(), "RPC failed") == 0){
        clientError(io->fd, "Response: >" + response + "<", 500, "RPC Error", "RPC has failed.");
        return -1;
    }
    string sentContentStr = bigTableClient.get(sender, "sent");
    if(strcasecmp(sentContentStr.c_str(), "RPC failed") == 0){
        clientError(io->fd, sentContentStr, 400, "no such path", "There is no such path (sent) for this user: " + sender);
        return -1;
    }
    Json::Value sentContent;
    if(!reader.parse(sentContentStr.c_str(), sentContent)){
        clientError(io->fd, sentContentStr, 500, "json error", "Content is not stored in json format");
        return -1;
    }
    sentContent.append(mailContentKeyStr);
    sentContentStr = writer.write(sentContent);
    response = bigTableClient.put(sender, "sent", sentContentStr);
    if(strcasecmp(response.c_str(), "RPC failed") == 0){
        clientError(io->fd, "Response: >" + response + "<", 500, "RPC Error", "RPC has failed.");
        return -1;
    }
    responseBuilder << "SUCCESS Added mail to sender's sent folder" << "\r\n";

    sendResponse(io, map<string, string>(), responseBuilder.str(), requestMessage);
    return 0;
}

/*
 * CGI for getFile
 */
int CGI_GET_FILE(IO *io, map<string, string> CGIArguments, HTTPRequest *requestMessage){
    string sessionId;
    if(!isLoggedIn(requestMessage, sessionId)){
        log("user is not currently logged in");
        sendResponse(io, map<string, string>(), "You are not logged in!", requestMessage);
        return -1;
    }
    string username = sessionBook[sessionId].username;

    stringstream headerBuilder;
    stringstream bodyBuilder;

    string currentPath;
    if(CGIArguments.find("path") == CGIArguments.end()){
        log("path argument not found in CGI Arguments");
        currentPath = "root";
    }
    else{
        currentPath = CGIArguments["path"];
    }
    log("Current path is: " + currentPath);
    string contentInPath = bigTableClient.get(username, currentPath);
    map<string, string> subFolders;
    map<string, string> subFiles;
    if(strcasecmp(contentInPath.c_str(), "RPC failed") == 0){
        clientError(io->fd, contentInPath, 400, "no such path", "There is no such path for this user: " + username);
        return -1;
    }
    else{
        Json::Value root;
        Json::Reader reader;
        if(!reader.parse(contentInPath.c_str(), root)){
            clientError(io->fd, contentInPath, 500, "json error", "Content is not stored in json format");
            return -1;
        }
        bool isFolder = root["type"].asBool();
        if(isFolder){
            for(Json::ValueIterator it = root["folders"].begin() ; it != root["folders"].end() ; it ++){
                subFolders[it.key().asString()] = it->asString();
            }
            for(Json::ValueIterator it = root["files"].begin() ; it != root["files"].end() ; it ++){
                subFiles[it.key().asString()] = it->asString();
            }
            bodyBuilder << "<html>" << "\r\n";
            bodyBuilder << "<head>" << "\r\n";
            bodyBuilder << "<title>" << currentPath << "</title>" << "\r\n";
            bodyBuilder << "<script src=\"https://ajax.googleapis.com/ajax/libs/jquery/3.2.0/jquery.min.js\"></script>" << "\r\n";
            bodyBuilder << "<script src=\"/js/sendFile.js\"></script>" << "\r\n";
            bodyBuilder << "</head>" << "\r\n";
            bodyBuilder << "<body>\r\n";
            bodyBuilder << "Current Path: " << currentPath << "\r\n";
            bodyBuilder << "<hr>" << "\r\n";
            bodyBuilder << "Sub Folders:<br>" << "\r\n";
            for(map<string, string>::iterator it = subFolders.begin() ; it != subFolders.end() ; it ++){
                bodyBuilder << "<form action=\"/cgi-bin/getFile\" method=\"get\">" << "\r\n";
                bodyBuilder << "<input type=hidden name=\"path\" value=\"" << it->first << "\">" << "\r\n";
                bodyBuilder << "<button type=\"submit\">" << it->second << "</button>" << "\r\n";
                bodyBuilder << "</form><br>" << "\r\n";

                bodyBuilder << "<form action=\"/cgi-bin/deleteFolder\" method=\"get\">" << "\r\n";
                bodyBuilder << "<input type=hidden name=\"source\" value=\"" << currentPath << "\">" << "\r\n";
                bodyBuilder << "<input type=hidden name=\"path\" value=\"" << it->first << "\">" << "\r\n";
                bodyBuilder << "<button type=\"submit\">" << "Delete Folder" << "</button>" << "\r\n";
                bodyBuilder << "</form><br>" << "\r\n";
            }
            bodyBuilder << "<hr>" << "\r\n";
            bodyBuilder << "Sub Files:<br>" << "\r\n";
            for(map<string, string>::iterator it = subFiles.begin() ; it != subFiles.end() ; it ++){
                bodyBuilder << "<form action=\"/cgi-bin/getFile\" method=\"get\">" << "\r\n";
                bodyBuilder << "<input type=hidden name=\"path\" value=\"" << it->first << "\">" << "\r\n";
                bodyBuilder << "<button type=\"submit\">" << it->second << "</button>" << "\r\n";
                bodyBuilder << "</form><br>" << "\r\n";

                bodyBuilder << "<form action=\"/cgi-bin/deleteFile\" method=\"get\">" << "\r\n";
                bodyBuilder << "<input type=hidden name=\"source\" value=\"" << currentPath << "\">" << "\r\n";
                bodyBuilder << "<input type=hidden name=\"path\" value=\"" << it->first << "\">" << "\r\n";
                bodyBuilder << "<button type=\"submit\">" << "Delete File" << "</button>" << "\r\n";
                bodyBuilder << "</form><br>" << "\r\n";

                bodyBuilder << "<form action=\"/cgi-bin/renameFile\" method=\"get\">" << "\r\n";
                bodyBuilder << "<input type=hidden name=\"path\" value=\"" << it->first << "\">" << "\r\n";
                bodyBuilder << "<input type=hidden name=\"source\" value=\"" << currentPath << "\">" << "\r\n";
                bodyBuilder << "<input type=text name=\"newName\">" << "\r\n";
                bodyBuilder << "<button type=\"submit\">" << "Rename" << "</button>" << "\r\n";
                bodyBuilder << "</form><br>" << "\r\n";

                bodyBuilder << "<form action=\"/cgi-bin/moveFile\" method=\"get\">" << "\r\n";
                bodyBuilder << "<input type=hidden name=\"source\" value=\"" << currentPath << "\">" << "\r\n";
                bodyBuilder << "<input type=hidden name=\"file\" value=\"" << it->first << "\">" << "\r\n";
                bodyBuilder << "<input type=text name=\"destination\">" << "\r\n";
                bodyBuilder << "<button type=\"submit\">" << "Move File" << "</button>" << "\r\n";
                bodyBuilder << "</form><br>" << "\r\n";

                bodyBuilder << "<form action=\"/cgi-bin/shareFile\" method=\"post\">" << "\r\n";
                bodyBuilder << "<input type=hidden name=\"filepath\" value=\"" << it->first << "\">" << "\r\n";
                bodyBuilder << "<input type=hidden name=\"filename\" value=\"" << it->second << "\">" << "\r\n";
                bodyBuilder << "<input type=text name=\"destuser\">" << "\r\n";
                bodyBuilder << "<button type=\"submit\">Share</button>" << "\r\n";
                bodyBuilder << "</form><br>" << "\r\n";
            }
            bodyBuilder << "<hr>" << "\r\n";
            bodyBuilder << "<form action=\"/cgi-bin/createFolder\" method=\"post\">" << "\r\n";
            bodyBuilder << "<input type=hidden name=\"path\" value=\"" << currentPath << "\">" << "\r\n";
            bodyBuilder << "<p><input type=\"text\" name=\"foldername\" id=\"foldername\">" << "\r\n";
            bodyBuilder << "<p><button type=\"submit\">Create Folder</button>" << "\r\n";
            bodyBuilder << "</form>" << "\r\n";

            bodyBuilder << "<hr>" << "\r\n";
            bodyBuilder << "<form action=\"/cgi-bin/sendFile\" method=\"post\" enctype=\"multipart/form-data\" id=\"send-file-form\">" << "\r\n";
            bodyBuilder << "<input type=hidden name=\"path\" id=\"path\" value=\"" << currentPath << "\">" << "\r\n";
            bodyBuilder << "<p><input type=\"file\" name=\"fileToUpload\" id=\"fileToUpload\">" << "\r\n";
            bodyBuilder << "<p><button type=\"submit\">Upload File</button>" << "\r\n";
            bodyBuilder << "</form>" << "\r\n";
            bodyBuilder << "</body>" << "\r\n";
            bodyBuilder << "</html>" << "\r\n";

            sendResponse(io, map<string, string>(), bodyBuilder.str(), requestMessage);
        }
        else{
            string body = root["content"].asString();
            vector<BYTE> decodedData = base64_decode(body);
            string fileName = root["filename"].asString();
            int fileSize = decodedData.size();
            map<string, string> header;
            header["Content-Description"] = "File Transfer";
            header["Content-Type"] = getFileType(fileName) + ";charset=UTF-8";
            header["Content-Length"] = to_string(fileSize);
            header["Content-Disposition"] = "attachment; filename=\"" + fileName + "\"";
            header["Content-Transfer-Encoding"] = "base64";

            sendResponse(io, header, decodedData, requestMessage);
        }
    }
}

/*
 * Shares file with the user passed in CGI
 */
int CGI_SHARE_FILE(IO *io, map<string, string> CGIArguments, HTTPRequest *requestMessage){
    string filePath, sourceUser, destUser;
    string sessionId;
    Json::Value file;
    string filename;
    Json::FastWriter writer;

    if(!isLoggedIn(requestMessage, sessionId)){
        log("user is not currently logged in");
        sendResponse(io, map<string, string>(), "You are not logged in!", requestMessage);
        return -1;
    }
    sourceUser = sessionBook[sessionId].username;

    if(CGIArguments.find("filepath") == CGIArguments.end()){
        clientError(io->fd, "filepath", 400, "File path not found", "CGI Arguments does not contain filePath argument.");
        return -1;
    }
    else{
        filePath = CGIArguments["filepath"];
        string contentInPath;
        if(doesExists(sourceUser, filePath, contentInPath)){
            Json::Reader reader;
            if(!reader.parse(contentInPath.c_str(), file)){
                clientError(io->fd, contentInPath, 500, "json error", "Content is not stored in json format");
                return -1;
            }
            if(file["type"].asBool()){
                clientError(io->fd, filePath, 500, "Its a dir", "sharing a folder not allowed");
                return -1;
            }
        }
        else{
            clientError(io->fd, contentInPath, 500, "dir does not exist", "This condition means that file path which user wanted to share did not exist. Behaviour unexpected !!!");
            return -1;
        }
    }

    if(CGIArguments.find("destuser") == CGIArguments.end()){
        clientError(io->fd, "destuser", 400, "Dest User not found", "CGI Arguments does not contain destUser argument");
        return -1;
    }
    else{
        destUser = CGIArguments["destuser"];
        string password;
        if(!doesExists(destUser, "password", password)){
            clientError(io->fd, "DestUser: --" + destUser + "--", 400, "Dest User empty", "Please provide a valid destUser");
            return -1;
        }
    }

    if(CGIArguments.find("filename") == CGIArguments.end()){
        clientError(io->fd, "filename", 400, "file name not found", "CGI Arguments does not contain filename argument");
        return -1;
    }
    else{
        filename = CGIArguments["filename"];
    }

    if(strcasecmp(destUser.c_str(), sourceUser.c_str()) == 0){
        clientError(io->fd, "DestUser: <" + destUser + "> SourceUser: <" + sourceUser + ">", 400, "Dest User and Source Users are same", "Cannot share file with yourself!!!");
        return -1;
    }
    
    string contentInPath;
    Json::Value rootDir;
    if(doesExists(destUser, "root", contentInPath)){
        Json::Reader reader;
        if(!reader.parse(contentInPath.c_str(), rootDir)){
            clientError(io->fd, contentInPath, 500, "json error", "Content is not stored in json format");
            return -1;
        }
        if(!rootDir["type"].asBool()){
            clientError(io->fd, contentInPath, 500, "Its a file", "root folder has to be a file");
            return -1;
        }
        string destFilePathRoot = "shared_" + sourceUser;
        string destFilePath = destFilePathRoot;
        string existance;
        int i = 1;
        while(doesExists(destUser, destFilePath, existance)){
            destFilePath = destFilePathRoot + "_" + to_string(i);
            i += 1;
        }
        string fileContent = writer.write(file);
        string response = bigTableClient.put(destUser, destFilePath, fileContent);
        if(strcasecmp(response.c_str(), "RPC failed") == 0){
            clientError(io->fd, destFilePath, 500, "RPC Error", "RPC has failed.");
            return -1;
        }
        else{
            rootDir["files"][destFilePath] = filename;
            string rootDirContent = writer.write(rootDir);
            response = bigTableClient.put(destUser, "root", rootDirContent);
            if(strcasecmp(response.c_str(), "RPC failed") == 0){
                clientError(io->fd, "root: " + rootDirContent, 500, "RPC Error", "RPC has failed.");
                return -1;
            }
            else{
                sendResponse(io, map<string, string>(), destFilePath + ": " + filename + "; sourceUser: " + sourceUser + " file has been shared and " + destUser + "'s root folder list has been updated", requestMessage);
            }
        }
    }
    else{
        clientError(io->fd, "DestUser: <" + destUser + ">: root", 400, "Dest User root does not exist", "Root directory for " + destUser + " does not exist");
        return -1;
    }
    return 0;
}

/*
 * Creates folder in the directory passed in CGI
 */
int CGI_CREATE_FOLDER(IO *io, map<string, string> CGIArguments, HTTPRequest *requestMessage){
    string dirPath, user, folderName;
    string sessionId;
    Json::Value dir;

    if(!isLoggedIn(requestMessage, sessionId)){
        log("user is not currently logged in");
        sendResponse(io, map<string, string>(), "You are not logged in!", requestMessage);
        return -1;
    }
    user = sessionBook[sessionId].username;

    if(CGIArguments.find("path") == CGIArguments.end()){
        clientError(io->fd, "path", 400, "dir path not found", "CGI Arguments does not contain dir path argument.");
        return -1;
    }
    else{
        dirPath = CGIArguments["path"];
        Json::Reader reader;
        string contentInPath;
        if(doesExists(user, dirPath, contentInPath)){
            if(!reader.parse(contentInPath.c_str(), dir)){
                clientError(io->fd, contentInPath, 500, "json error", "Content is not stored in json format");
                return -1;
            }
            if(!dir["type"].asBool()){
                clientError(io->fd, dirPath, 500, "Its a file", "Making a folder in file");
                return -1;
            }
        }
        else{
            clientError(io->fd, contentInPath, 500, "dir does not exist", "This condition means that dir path in which user wanted to create a new folder did not exist. Behaviour unexpected !!!");
            return -1;
        }
    }

    if(CGIArguments.find("foldername") == CGIArguments.end()){
        clientError(io->fd, "foldername", 400, "Folder name not found", "CGI Arguments does not contain foldername argument");
        return -1;
    }
    else{
        folderName = CGIArguments["foldername"];
        if(trim_copy(folderName).size() == 0){
            clientError(io->fd, "foldername:>" + folderName + "<", 400, "Folder name empty", "Folder name length cannot be 0.");
            return -1;
        }
    }
    string folderPath = dirPath + "_" + folderName;
    string response;
    if(!doesExists(user, folderPath, response)){
        if(strcasecmp(response.c_str(), "RPC failed") == 0){
            clientError(io->fd, folderPath, 500, "rpc failed", "RPC has failed!");
            return -1;
        }
        else{
            Json::Value folder;
            Json::FastWriter writer;

            folder["type"] = true;
            folder["files"] = Json::Value();
            folder["folders"] = Json::Value();

            string folderContent = writer.write(folder);
            log(folderPath + ": " + folderContent);
            response = bigTableClient.put(user, folderPath, folderContent);
            if(strcasecmp(response.c_str(), "RPC failed") == 0){
                clientError(io->fd, folderPath + ":" + folderContent, 500, "RPC Error", "RPC has failed.");
                return -1;
            }
            else{
                dir["folders"][folderPath] = folderName;
                folderContent = writer.write(dir);
                response = bigTableClient.put(user, dirPath, folderContent);
                if(strcasecmp(response.c_str(), "RPC failed") == 0){
                    clientError(io->fd, dirPath + ":" + folderContent,  500, "RPC Error", "RPC has failed.");
                    return -1;
                }
                else{
                    sendResponse(io, map<string, string>(), folderPath + ":" + folderContent + "; sourceUser: " + user + " folder has been created and current folder list has been updated", requestMessage);
                }
            }
            return 0;
        }
    }
    else{
        clientError(io->fd, folderPath, 400, "Folder name already exists", "A folder name makes the folder path which already exists for this user. Please choose another foldername");
        return -1;
    }
}

/*
 * CGI for sendFile
 */
int CGI_SEND_FILE(IO *io, string boundary, HTTPRequest *requestMessage){
    map<string, string> multipartText;
    map<string, string> multipartFile;
    parseMultipartFormData(io, boundary, requestMessage, multipartText, multipartFile);
    stringstream body;
    for(auto it = multipartText.begin() ; it != multipartText.end() ; it ++){
        body << it->first << ": " << it->second << "<br>";
    }
    for(auto it = multipartFile.begin() ; it != multipartFile.end() ; it ++){
        body << it->first << ": " << it->second << "<br>";
    }
    string sessionId;

    if(!isLoggedIn(requestMessage, sessionId)){
        log("user is not currently logged in");
        sendResponse(io, map<string, string>(), "You are not logged in!", requestMessage);
        return -1;
    }
    string user = sessionBook[sessionId].username;
    string dirPath;
    if(multipartText.find("path") == multipartText.end()){
        clientError(io->fd, "dirPath", 400, "path not found", "CGI Argument does not contain path variable!!!");
        return -1;
    }
    else{
        dirPath = multipartText["path"];
    }
    for(map<string, string>::iterator it = multipartFile.begin() ; it != multipartFile.end() ; it ++){
        string fileName = it->first;
        string fileContent = it->second;
        string filePathRoot = dirPath + "_" + fileName;
        string filePath = filePathRoot;
        string response;
        Json::FastWriter writer;
        int i = 1;
        while(doesExists(user, filePath, response)){
            filePath = filePathRoot + "_" + to_string(i);
            i += 1;
        }
        Json::Value fileContentJson;
        fileContentJson["type"] = false;
        fileContentJson["filename"] = fileName;
        fileContentJson["content"] = fileContent;
        fileContent = writer.write(fileContentJson);
        response = bigTableClient.put(user, filePath, fileContent);
        if(strcasecmp(response.c_str(), "RPC failed") == 0){
            clientError(io->fd, fileName,  500, "RPC Error", "RPC has failed.");
            return -1;
        }
        else{
            if(doesExists(user, dirPath, response)){
                Json::Reader reader;
                Json::Value dir;
                if(!reader.parse(response.c_str(), dir)){
                    clientError(io->fd, response, 500, "json error", "Content is not stored in json format");
                    return -1;
                }
                if(!dir["type"].asBool()){
                    clientError(io->fd, dirPath, 500, "Its a file", "Trying to upload file in a file. Not possible!");
                    return -1;
                }
                dir["files"][filePath] = fileName;
                string dirContent = writer.write(dir);
                response = bigTableClient.put(user, dirPath, dirContent);
                if(strcasecmp(response.c_str(), "RPC failed") == 0){
                    clientError(io->fd, dirPath + ":" + dirContent,  500, "RPC Error", "RPC has failed.");
                    return -1;
                }
                else{
                    sendResponse(io, map<string, string>(), dirPath + ":" + dirContent + ";" + filePath + ": " + fileName + " file has been uploaded and directory's file list has been updated", requestMessage);
                    return 0;
                }
            }
            else{
                clientError(io->fd, dirPath + ">>" + response + "<<",  500, "dir path does not exist", "This error means that dir path does not exist for user:" + user);

                return -1;
            }
        }
    }
}

/*
 * Delete folder
 */
int CGI_DELETE_FOLDER(IO *io, map<string, string> parsedArguments, HTTPRequest *requestMessage){
    stringstream bodyBuilder;
    string sessionId;

    if(!isLoggedIn(requestMessage, sessionId)){
        log("user is not currently logged in");
        sendResponse(io, map<string, string>(), "You are not logged in!", requestMessage);
        return -1;
    }
    string user = sessionBook[sessionId].username;

    if(parsedArguments.find("path") == parsedArguments.end()){
        clientError(io->fd, "path", 400, "path not found", "CGI Argument does not contain folder path to delete variable!!!");
        return -1;
    }
    string path = parsedArguments["path"];
    if(parsedArguments.find("source") == parsedArguments.end()){
        clientError(io->fd, "source", 400, "source not found", "CGI Argument does not contain folder source to delete from variable!!!");
        return -1;
    }
    string source = parsedArguments["source"];
    int retValue = deleteFolder(io, user, source, path);
    if(retValue == 0){
        bodyBuilder << "Folder has been successfully deleted!!!" << "\r\n";
        sendResponse(io, map<string, string>(), bodyBuilder.str(), requestMessage);
    }
}

/*
 * Delete file
 */
int CGI_DELETE_FILE(IO *io, map<string, string> parsedArguments, HTTPRequest *requestMessage){
    stringstream bodyBuilder;
    for(map<string, string>::iterator it = parsedArguments.begin() ; it != parsedArguments.end() ; it ++){
        bodyBuilder << it->first << "->" << it->second << "<br>";
    }
    string sessionId;

    if(!isLoggedIn(requestMessage, sessionId)){
        log("user is not currently logged in");
        sendResponse(io, map<string, string>(), "You are not logged in!", requestMessage);
        return -1;
    }
    string user = sessionBook[sessionId].username;

    if(parsedArguments.find("path") == parsedArguments.end()){
        clientError(io->fd, "path", 400, "path not found", "CGI Argument does not contain folder path to delete variable!!!");
        return -1;
    }
    string path = parsedArguments["path"];
    if(parsedArguments.find("source") == parsedArguments.end()){
        clientError(io->fd, "source", 400, "source not found", "CGI Argument does not contain folder source to delete from variable!!!");
        return -1;
    }
    string source = parsedArguments["source"];
    deleteFile(io, user, source, path);
    bodyBuilder << "File has been successfully deleted" << "\r\n";
    sendResponse(io, map<string, string>(), bodyBuilder.str(), requestMessage);
}

/*
 * rename file
 */
int CGI_RENAME_FILE(IO *io, map<string, string> parsedArguments, HTTPRequest *requestMessage){
    // TODO
    stringstream bodyBuilder;
    for(map<string, string>::iterator it = parsedArguments.begin() ; it != parsedArguments.end() ; it ++){
        bodyBuilder << it->first << "->" << it->second << "<br>";
    }
    string sessionId;

    if(!isLoggedIn(requestMessage, sessionId)){
        log("user is not currently logged in");
        sendResponse(io, map<string, string>(), "You are not logged in!", requestMessage);
        return -1;
    }
    string user = sessionBook[sessionId].username;

    if(parsedArguments.find("source") == parsedArguments.end()){
        clientError(io->fd, "source", 400, "source not found", "CGI Argument does not contain folder source variable!!!");
        return -1;
    }
    string source = parsedArguments["source"];

    if(parsedArguments.find("newName") == parsedArguments.end()){
        clientError(io->fd, "newName", 400, "newName not found", "CGI Argument does not contain newName variable!!!");
        return -1;
    }
    string newName = parsedArguments["newName"];

    if(parsedArguments.find("path") == parsedArguments.end()){
        clientError(io->fd, "path", 400, "path not found", "CGI Argument does not contain file path variable!!!");
        return -1;
    }
    string path = parsedArguments["path"];

    Json::FastWriter writer;
    Json::Reader reader;
    string sourceContentStr;
    Json::Value sourceContent;
    string fileContentStr;
    Json::Value fileContent;

    if(!doesExists(user, source, sourceContentStr)){
        clientError(io->fd, source, 400, "invalid folder", "no such entry in big table");
        return -1;
    }
    if(!reader.parse(sourceContentStr.c_str(), sourceContent)){
        clientError(io->fd, sourceContentStr, 500, "json error", "Content is not stored in json format");
        return -1;
    }
    if(!sourceContent["type"].asBool()){
        clientError(io->fd, sourceContentStr, 500, "Its a file", "Should be a folder!!!");
        return -1;
    }

    if(!doesExists(user, path, fileContentStr)){
        clientError(io->fd, path, 400, "invalid file", "no such entry in big table");
        return -1;
    }
    if(!reader.parse(fileContentStr.c_str(), fileContent)){
        clientError(io->fd, fileContentStr, 500, "json error", "Content is not stored in json format");
        return -1;
    }
    if(fileContent["type"].asBool()){
        clientError(io->fd, fileContentStr, 500, "Its a folder", "Trying to rename folder . Wrong request. Should not happen!!!");
        return -1;
    }

    fileContent["filename"] = newName;
    fileContentStr = writer.write(fileContent);
    bigTableClient.put(user, path, fileContentStr);
    log("putting " + fileContentStr + " in " + user + "," + path);

    Json::Value subFiles = sourceContent["files"];
    subFiles[path] = newName;
    sourceContent["files"] = subFiles;
    sourceContentStr = writer.write(sourceContent);
    bigTableClient.put(user, source, sourceContentStr);
    log("putting " + sourceContentStr + " in " + user + "," + source);

    bodyBuilder << "File has been successfully renamed!!!";

    sendResponse(io, map<string, string>(), bodyBuilder.str(), requestMessage);
}

/*
 * move file
 */
int CGI_MOVE_FILE(IO *io, map<string, string> parsedArguments, HTTPRequest *requestMessage){
    stringstream bodyBuilder;
    for(map<string, string>::iterator it = parsedArguments.begin() ; it != parsedArguments.end() ; it ++){
        log(it->first + " " + it->second);
    }
    string sessionId;

    if(!isLoggedIn(requestMessage, sessionId)){
        log("user is not currently logged in");
        sendResponse(io, map<string, string>(), "You are not logged in!", requestMessage);
        return -1;
    }
    string user = sessionBook[sessionId].username;

    if(parsedArguments.find("source") == parsedArguments.end()){
        clientError(io->fd, "source", 400, "source not found", "CGI Argument does not contain folder source variable!!!");
        return -1;
    }
    string source = parsedArguments["source"];
    if(parsedArguments.find("destination") == parsedArguments.end()){
        clientError(io->fd, "destination", 400, "destination not found", "CGI Argument does not contain folder destination variable!!!");
        return -1;
    }
    string destination = parsedArguments["destination"];
    if(parsedArguments.find("file") == parsedArguments.end()){
        clientError(io->fd, "file", 400, "file not found", "CGI Argument does not contain file path variable!!!");
        return -1;
    }
    string path = parsedArguments["file"];

    Json::FastWriter writer;
    Json::Reader reader;
    string sourceContentStr;
    Json::Value sourceContent;
    string destinationContentStr;
    Json::Value destinationContent;
    string fileContentStr;
    Json::Value fileContent;

    if(!doesExists(user, source, sourceContentStr)){
        clientError(io->fd, source, 400, "invalid folder", "no such entry in big table");
        return -1;
    }
    if(!reader.parse(sourceContentStr.c_str(), sourceContent)){
        clientError(io->fd, sourceContentStr, 500, "json error", "Content is not stored in json format");
        return -1;
    }
    if(!sourceContent["type"].asBool()){
        clientError(io->fd, sourceContentStr, 500, "Its a file", "Trying to move something from file . Wrong request. Should not happen!!!");
        return -1;
    }

    if(!doesExists(user, destination, destinationContentStr)){
        clientError(io->fd, source, 400, "invalid folder", "no such entry in big table");
        return -1;
    }
    if(!reader.parse(destinationContentStr.c_str(), destinationContent)){
        clientError(io->fd, sourceContentStr, 500, "json error", "Content is not stored in json format");
        return -1;
    }
    if(!destinationContent["type"].asBool()){
        clientError(io->fd, destinationContentStr, 500, "Its a file", "Trying to move something to file . Wrong request. Should not happen!!!");
        return -1;
    }

    if(!doesExists(user, path, fileContentStr)){
        clientError(io->fd, path, 400, "invalid file", "no such entry in big table");
        return -1;
    }
    if(!reader.parse(fileContentStr.c_str(), fileContent)){
        clientError(io->fd, fileContentStr, 500, "json error", "Content is not stored in json format");
        return -1;
    }
    if(fileContent["type"].asBool()){
        clientError(io->fd, destinationContentStr, 500, "Its a folder", "Trying to move folder . Wrong request. Should not happen!!!");
        return -1;
    }

    Json::Value subFiles = destinationContent["files"];
    subFiles[path] = sourceContent["files"][path];
    destinationContent["files"] = subFiles;
    destinationContentStr = writer.write(destinationContent);
    bigTableClient.put(user, destination, destinationContentStr);
    log("adding to destination: " + destinationContentStr);

    subFiles = sourceContent["files"];
    subFiles.removeMember(path);
    sourceContent["files"] = subFiles;
    sourceContentStr = writer.write(sourceContent);
    bigTableClient.put(user, source, sourceContentStr);
    log("removing from source: " + sourceContentStr);

    bodyBuilder << "File has been successfully moved";
    sendResponse(io, map<string, string>(), bodyBuilder.str(), requestMessage);
}

/*
 * CGI-Script for Admin console 
 */
int CGI_ADMIN(IO *io, map<string, string> parsedArguments, HTTPRequest *requestMessage){
    stringstream bodyBuilder;
    bodyBuilder << "<html>" << "\r\n";
    bodyBuilder << "<head>" << "\r\n";
    bodyBuilder << "<title> Admin </title>" << "\r\n";
    bodyBuilder << "</head>" << "\r\n";
    bodyBuilder << "<body>" << "\r\n";
    bodyBuilder << "Front End Servers:" << "\r\n";
    for(vector<string>::iterator it = httpServersInAction.begin() ; it != httpServersInAction.end() ; it ++){
        bodyBuilder << "<form action=\"/cgi-bin/killServer\" method=\"post\">" << "\r\n";
        bodyBuilder << *it << "<input type=hidden name=\"port\" value=\"" << (*it) << "\">" << "\r\n";
        bodyBuilder << "<input type=hidden name=\"type\" value=\"" << "frontend" << "\">" << "\r\n";
        bodyBuilder << "<button type=\"submit\">" << "Kill" << "</button>" << "\r\n";
        bodyBuilder << "</form><br>" << "\r\n";
    }
    bodyBuilder << "<hr>" << "\r\n";
    bodyBuilder << "<form action=\"/cgi-bin/startServer\" method=\"post\">" << "\r\n";
    bodyBuilder << "Location: <input type=text name=\"port\">" << "\r\n";
    bodyBuilder << "<input type=hidden name=\"type\" value=\"" << "frontend" << "\">" << "\r\n";
    bodyBuilder << "<button type=\"submit\">" << "Start" << "</button>" << "\r\n";
    bodyBuilder << "</form><br>" << "\r\n";
    bodyBuilder << "<hr>" << "\r\n";
    bodyBuilder << "</body>" << "\r\n";
    bodyBuilder << "</html>" << "\r\n";

    sendResponse(io, map<string, string>(), bodyBuilder.str(), requestMessage);
}

/*
 * CGI-Script for Admin console 
 */
int CGI_START_SERVER(IO *io, map<string, string> parsedArguments, HTTPRequest *requestMessage){
    stringstream bodyBuilder;
    int port;
    if(parsedArguments.find("type") == parsedArguments.end()){
        clientError(io->fd, "type", 400, "type not found", "No type argument in CGI arguments.");
        return -1;
    }
    if(parsedArguments.find("port") == parsedArguments.end()){
        clientError(io->fd, "port", 400, "no port found", "No port argument in CGI arguments.");
        return -1;
    }
    else{
        port = stoi(parsedArguments["port"]);
        log("Starting server at " + to_string(port));
    }
    if(strcasecmp(parsedArguments["type"].c_str(), "frontend") == 0){
        log("forking");
        if(fork() == 0){
            // child process
            char bufferHTTP[5], bufferLOAD[5];
            snprintf(bufferHTTP, sizeof(bufferHTTP), "%d", port);
            snprintf(bufferLOAD, sizeof(bufferLOAD), "%d", LOAD_BALANCER_PORT);
            char *argv[] = {"-v", "-h", bufferHTTP, "-l", bufferLOAD, NULL};
            log("starting http server");
            int response = execv("./httpserver", argv);
            log("failed exec");
        }
        bodyBuilder << "Started server at : " << "localhost:" << to_string(port);
        sendResponse(io, map<string, string> (), bodyBuilder.str(), requestMessage);
    }
    else if(strcasecmp(parsedArguments["type"].c_str(), "backend") == 0){
        // TODO
    }
    else{
        clientError(io->fd, "type: " + parsedArguments["type"],  400, "wrong type", "type can either be frontend or backend.");
        return -1;
    }
}

/*
 * CGI-Script for Admin console 
 */
int CGI_KILL_SERVER(IO *io, map<string, string> parsedArguments, HTTPRequest *requestMessage){
    stringstream bodyBuilder;
    int port;
    if(parsedArguments.find("type") == parsedArguments.end()){
        clientError(io->fd, "type", 400, "type not found", "No type argument in CGI arguments.");
        return -1;
    }
    if(parsedArguments.find("port") == parsedArguments.end()){
        clientError(io->fd, "port", 400, "no port found", "No port argument in CGI arguments.");
        return -1;
    }
    else{
        port = stoi(parsedArguments["port"]);
        log("Starting server at " + to_string(port));
    }
    if(strcasecmp(parsedArguments["type"].c_str(), "frontend") == 0){
        int sockfd = socket(PF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
            warning("Could not create socket for killing server at " + to_string(port));
        }
        struct sockaddr_in servaddr;
        bzero(&servaddr, sizeof(servaddr));
        servaddr.sin_family = AF_INET;
        servaddr.sin_port = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &(servaddr.sin_addr));
        connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr));
        log("sending quit to server at port " + to_string(port));
        writeIO(sockfd, "quit", sizeof("quit"));
        close(sockfd);
        bodyBuilder << "serer stopped at port " << port;
        sendResponse(io, map<string, string> (), bodyBuilder.str(), requestMessage);
    }
    else if(strcasecmp(parsedArguments["type"].c_str(), "backend") == 0){
        // TODO
    }
    else{
        clientError(io->fd, "type: " + parsedArguments["type"],  400, "wrong type", "type can either be frontend or backend.");
        return -1;
    }
}

/*
 * Parses URI
 * Returns 0 if static content
 *         1 if dynamic
 *        -1 if error
 */
int parseURI(IO *io, string uri, string &fileName, string &CGIArguments){
    dlog("Parsing URI");
    int cgiPos = uri.find(CGI_DIR);
    if(cgiPos == string::npos){
        // static
        dlog("Static Request");
        CGIArguments = "";
        fileName = (fs::path) WWW_DIR / (fs::path) uri;
        if(!fs::exists((fs::path) fileName)){
            clientError(io->fd, fileName, 404, "File not found", "Could not locate the resource");
            return -1;
        } 
        return 0;
    }
    else{
        // dynamic
        dlog("Dynamic Request");
        int QPos = uri.find("?");
        if(QPos == string::npos){
            fileName = uri;
            CGIArguments = "";
        }
        else{
            fileName = uri.substr(0, QPos);
            CGIArguments = uri.substr(QPos + 1);
        }
        if(!isCorrectDynamicRequestFile(fileName)){
            clientError(io->fd, fileName, 404, "File not found-2", "Could not locate the resource");
            return -1;
        }
        return 1;
    }
}

/*
 * Serves static content for get requests
 */
void serveStaticContent(IO *io, HTTPRequest *requestMessage, string fileName){
    int fileSize = fs::file_size((fs::path)fileName);
    stringstream headerBuilder;
    headerBuilder << "HTTP/1.0 200 OK\r\n";
    headerBuilder << "Content-Type: " << getFileType(fileName) << "\r\n";
    headerBuilder << "Content-Length: " << fileSize << "\r\n";
    headerBuilder << "\r\n";
    string header = headerBuilder.str();
    writeIO(io->fd, const_cast<char *>(header.c_str()), header.size());
    if(strcasecmp((requestMessage->method).c_str(), "HEAD") != 0){
        int fileFD = open(fileName.c_str(), O_RDONLY);
        if(fileFD < 0){
            warning("Could not open file: [" + fileName + "]");
            clientError(io->fd, "Could not open File", 403, "Permission Denied", "Unable to open file");
            return ;
        }
        void *filePointer = mmap(0, fileSize, PROT_READ, MAP_PRIVATE, fileFD, 0);
        if(filePointer == (caddr_t)(-1)){
            warning("Could not read file: [" + fileName + "]");
            clientError(io->fd, "Could not read File", 403, "Permission Denied", "Unable to read file");
            return ;
        }
        writeIO(io->fd, (char *)filePointer, fileSize);
        munmap(filePointer, fileSize);
    }
}

/*
 * Serves dynamic content for GET requests
 */
void serveGETDynamicContent(IO *io, string fileName, string contentType, string CGIArguments, HTTPRequest *requestMessage){
    if(strcasecmp(contentType.c_str(), "application/x-www-form-urlencoded") == 0){
        map<string, string> parsedArguments;
        if(CGIArguments.size() > 0){
            vector<string> CGIArgumentsComponents;
            split(CGIArgumentsComponents, CGIArguments, is_any_of("&"));
            log("CGIArguments: >>" + CGIArguments + "<<");
            log("CGI Arguments Size: " + to_string(CGIArgumentsComponents.size()));
            for(vector<string>::iterator it = CGIArgumentsComponents.begin() ; it != CGIArgumentsComponents.end() ; it ++){
                string argument = *it;
                log("argument: " + argument);
                int equalPos = argument.find("=");
                if(equalPos == string::npos){
                    clientError(io->fd, CGIArguments, 500, "Wrong Argument", "Equal Sign not found");
                    return ;
                }
                string key = argument.substr(0, equalPos);
                string value = argument.substr(equalPos + 1);
                parsedArguments[key] = value;
            }
        }
        if(strcmp(fileName.c_str(), "/cgi-bin/login") == 0){
            CGI_LOGIN(io, parsedArguments, requestMessage);
        }
        else if(strcmp(fileName.c_str(), "/cgi-bin/logout") == 0){
            CGI_LOGOUT(io, parsedArguments, requestMessage);
        }
        else if(strcmp(fileName.c_str(), "/cgi-bin/signup") == 0){
            CGI_SIGNUP(io, parsedArguments, requestMessage);
        }
        else if(strcmp(fileName.c_str(), "/cgi-bin/getMail") == 0){
            CGI_GET_MAIL(io, parsedArguments, requestMessage);
        }
        else if(strcmp(fileName.c_str(), "/cgi-bin/getFile") == 0){
            CGI_GET_FILE(io, parsedArguments, requestMessage);
        }
        else if(strcmp(fileName.c_str(), "/cgi-bin/shareFile") == 0){
            CGI_SHARE_FILE(io, parsedArguments, requestMessage);
        }
        else if(strcmp(fileName.c_str(), "/cgi-bin/createFolder") == 0){
            CGI_CREATE_FOLDER(io, parsedArguments, requestMessage);
        }
        else if(strcmp(fileName.c_str(), "/cgi-bin/admin") == 0){
            CGI_ADMIN(io, parsedArguments, requestMessage);
        }
        else if(strcmp(fileName.c_str(), "/cgi-bin/deleteFolder") == 0){
            CGI_DELETE_FOLDER(io, parsedArguments, requestMessage);
        }
        else if(strcmp(fileName.c_str(), "/cgi-bin/renameFile") == 0){
            CGI_RENAME_FILE(io, parsedArguments, requestMessage);
        }
        else if(strcmp(fileName.c_str(), "/cgi-bin/moveFile") == 0){
            CGI_MOVE_FILE(io, parsedArguments, requestMessage);
        }
        else if(strcmp(fileName.c_str(), "/cgi-bin/deleteFile") == 0){
            CGI_DELETE_FILE(io, parsedArguments, requestMessage);
        }
        else if(strcmp(fileName.c_str(), "/cgi-bin/startServer") == 0){
            CGI_START_SERVER(io, parsedArguments, requestMessage);
        }
        else if(strcmp(fileName.c_str(), "/cgi-bin/killServer") == 0){
            CGI_KILL_SERVER(io, parsedArguments, requestMessage);
        }
        else{
            warning("dynamic file does not exist. Should not occur. Already handled");
        }
    }
}

/*
 * Serves dynamic content for POST requests
 */
void servePOSTDynamicContent(IO *io, string fileName, HTTPRequest *requestMessage){
    string contentType = trim_copy(requestMessage->headers["content-type"]);
    int separationPos = contentType.find(";"); //position is obtained in case the form data is not multipart
    if(separationPos == string::npos){
        if(strcasecmp(contentType.c_str(), "application/x-www-form-urlencoded") == 0){
            char buffer[stoi(requestMessage->headers["content-length"]) + 1];
            readLineBufferedIO(io, buffer, stoi(requestMessage->headers["content-length"])); //reading the body of the request
            string str(buffer); //body stored in str string 
            serveGETDynamicContent(io, fileName, "application/x-www-form-urlencoded", str, requestMessage);
        }
        else{
            clientError(io->fd, contentType, 500, "Wrong content type", "separation position of ';' not found and contentn type was not x-www-form-urlencoded");
            return ;
        }
    }
    else{
        string boundary = trim_copy(contentType.substr(separationPos + 1));
        int equalPos = boundary.find("=");
        if(equalPos == string::npos){
            clientError(io->fd, boundary, 500, "Wrong boundary format", "separation position of '=' not found");
            return ;
        }
        else{
            boundary = boundary.substr(equalPos + 1);
        }
        contentType = trim_copy(contentType.substr(0, separationPos));
        if(strcasecmp(contentType.c_str(), "multipart/form-data") == 0){
            if(strcmp(fileName.c_str(), "/cgi-bin/sendMail") == 0){
                CGI_SEND_MAIL(io, boundary, requestMessage);
            }
            else if(strcmp(fileName.c_str(), "/cgi-bin/sendFile") == 0){
                CGI_SEND_FILE(io, boundary, requestMessage);
            }
            else{
                warning("dynamic file does not exist. Should not occur. Already handled");
            }
        }
        else{
            clientError(io->fd, contentType, 500, "Wrong content type", "separation position of ';' found and content type was not multipart form data");
            return ;
        }
    }
    return;
}

/*
 * Process Requests
 */
int processRequest(IO *io, HTTPRequest *requestMessage){
    if(!isPageAccessible(requestMessage)){
        redirect(io, "/index.html");
        return 0;
    }
    if(strcasecmp((requestMessage->method).c_str(), "HEAD") == 0){
        string fileName, CGIArguments;
        int pageType = parseURI(io, requestMessage->uri, fileName, CGIArguments);
        if(pageType == 0){
            serveStaticContent(io, requestMessage, fileName);
        }
        else if(pageType == 1){
            serveGETDynamicContent(io, fileName, "application/x-www-form-urlencoded", CGIArguments, requestMessage);
        }
        else{
            return -1;
        }
    }
    else if(strcasecmp((requestMessage->method).c_str(), "GET") == 0){
        string fileName, CGIArguments;
        int pageType = parseURI(io, requestMessage->uri, fileName, CGIArguments);
        if(pageType == 0){
            serveStaticContent(io, requestMessage, fileName);
        }
        else if(pageType == 1){
            serveGETDynamicContent(io, fileName, "application/x-www-form-urlencoded", CGIArguments, requestMessage);
        }
        else{
            return -1;
        }
    }
    else if(strcasecmp((requestMessage->method).c_str(), "POST") == 0){
        string fileName, CGIArguments;
        int pageType = parseURI(io, requestMessage->uri, fileName, CGIArguments);
        if(pageType == 0){
            clientError(io->fd, fileName, 404, "File not a script", "Wrong type of method for this purpose");
            return -1;
        }
        else if(pageType == 1){
            servePOSTDynamicContent(io, fileName, requestMessage);
        }
        else{
            return -1;
        }
    }
    return 0;
}

/*
 * Thread worker to handle one particular connection
 * Receives the string from connection until LF or CRLF encountered. 
 *        when encountered, runs the command
 * Sends the appropriate response back to the client.
 */
void *threadWorker(void *arg){
    pthread_detach(pthread_self());

    int fd = *(int *)arg;
    char buffer[MAX_MESSAGE_LEN];
    string line;
    HTTPRequest requestMessage;
    IO io;
    initIO(&io, fd);
    bool keepAlive = true;

    while(keepAlive){
        // read Request Line
        readLineBufferedIO(&io, buffer, MAX_MESSAGE_LEN);
        string requestLine = buffer;
        trim(requestLine);
        log("[" + to_string(fd) + "] Request Line received: [" + requestLine + "]");
        vector<string> requestLineComponents;
        split(requestLineComponents, requestLine, is_space());
        if(requestLineComponents.size() != 3){
            if(strcasecmp(requestLine.c_str(), "quit") == 0){
                signalSIGINTHandler(1);
            }
            for(auto c : requestLineComponents){
                warning(c);
            }
            warning("[" + to_string(fd) + "] Bad Request Line: [" + requestLine + "] Wrong number of components.");
            clientError(fd, requestLine, 400, "Bad Request Line", "Could not parse the request line. Wrong number of components");
            return NULL;
        }
        else{
            string method = requestLineComponents[0];
            string uri = requestLineComponents[1];
            string version = requestLineComponents[2];
            if(strcasecmp(method.c_str(), "GET") == 0){

            }
            else if(strcasecmp(method.c_str(), "POST") == 0){
                
            }
            else if(strcasecmp(method.c_str(), "HEAD") == 0){
                
            }
            else{
                warning("[" + to_string(fd) + "] Wrong method: [" + method + "] Not implemented.");
                clientError(fd, method, 501, "Wrong method", "Method not implemented. Implemented methods: GET, POST, HEAD");
                return NULL;
            }
            requestMessage.method = method;
            requestMessage.uri = uri;
            requestMessage.version = version;
        }

        // read request headers
        map<string, string> headers;
        string recentHeader = "";
        readLineBufferedIO(&io, buffer, MAX_MESSAGE_LEN);
        line = buffer;
        trim(line);
        while(line.size() != 0){
            log("[" + to_string(fd) + "] Header recieved: [" + line + "]");
            if(line[0] == ' ' || line[0] == '\t'){
                if(strcmp(recentHeader.c_str(), "") != 0){
                    headers[recentHeader] += "," + line;
                }
            }
            else{
                int colonPos = line.find(":");
                string key = line.substr(0, colonPos);
                to_lower(key);
                string value = line.substr(colonPos + 1);
                headers[key] = value;
                recentHeader = key;
            }
            readLineBufferedIO(&io, buffer, MAX_MESSAGE_LEN);
            line = buffer;
            trim(line);
        }
        requestMessage.headers = headers;

        if(headers.find("connection") == headers.end()){
            keepAlive = false;
        }
        else{
            if(strcasecmp(headers["connection"].c_str(), "keep-alive") == 0){
                keepAlive = true;
            }
            else{
                keepAlive = false;
            }
        }

        // process requests
        log("[" + to_string(fd) + "] Processing requests");
        processRequest(&io, &requestMessage);
    }

    log("[" + to_string(fd) + "] Connection closed.");
    allThreads.erase(fd);
    close(fd);
    return NULL;
}

/*
 * Starts the server, accepts connections and creates threads to handle those connections.
 */
int startServer(){
    log("Creating the socket");
    sockfd = socket(PF_INET, SOCK_STREAM, 0);
    if(sockfd < 0){
        panic("Socket Error: Could not start the server");
    }
    
    struct sockaddr_in servaddr;
    memset(&servaddr, '\0', sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port = htons(PORT_NO);
    log("Binding the socket to port " + to_string(PORT_NO));
    if(bind(sockfd, (struct sockaddr *) &servaddr, sizeof(servaddr)) < 0){
        panic("Bind Error: Could not bind the port");
    }
    
    log("Listening...");
    if(listen(sockfd, 10) < 0){
        panic("Listen Error: Could not listen...");
    }

    while(true){
        struct sockaddr_in clientaddr;
        socklen_t clientaddrlen = sizeof(clientaddr);
        int *fd = (int *) malloc(sizeof(int)); 
        *fd = accept(sockfd, (struct sockaddr *) &clientaddr, &clientaddrlen);
        if(*fd < 0){
            panic("Accept Error: Could not accept");
        }
        else{
            log("[" + to_string(*fd) + "] New Connection is created");
        }
        pthread_t thread;
        pthread_create(&thread, NULL, &threadWorker, fd);
        allThreads[*fd] = thread;
    }
    log("Server exited the loop");
    return 0;
}

int main(int argc, char *argv[]){    
    int c;
    char *val, *temp;
    while((c = getopt(argc, argv, "h:l:b:avd")) != -1){
        switch(c){
            case 'h': 
                val = optarg;
                PORT_NO = strtol(val, &temp, 0);
                if(*temp == '\0'){
                    if(PORT_NO <= 0){
                        panic("Port Number can not be less than 1.");
                    }
                }
                else{
                    panic("Port Number is not an integer.");
                }
                break;
            case 'l': 
                val = optarg;
                LOAD_BALANCER_PORT = strtol(val, &temp, 0);
                if(*temp == '\0'){
                    if(LOAD_BALANCER_PORT <= 0){
                        panic("Port Number can not be less than 1.");
                    }
                }
                else{
                    panic("Port Number is not an integer.");
                }
                break;
            case 'b': 
                BACKEND_SERVER = optarg;
                break;
            case 'a':
                panic("Devanshu Jain (devjain)");
            case 'v':
                V_FLAG = true;
                break;
            case 'd':
                DEEP_V_FLAG = true;
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

    struct sigaction sa;
    sa.sa_handler = sendHeartBeat;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    if(sigaction(SIGALRM, &sa, NULL) < 0){
        panic("Heartbeat handler could not be set up.");
    }
    else{
        log("Registering SIGALRM handler");
    }
    alarm(HEARTBEAT_INTERVAL_SECS);

    bigTableClient = KVSClientInterface(BACKEND_SERVER);

    startServer();

    return 0;
}