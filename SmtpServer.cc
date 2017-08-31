#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <fcntl.h>
#include <fstream>
#include <vector>

#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <memory>
#include <string>

#include "kvs_client.cc"
#include "json.h"
#include "MailService.h"

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/regex.hpp>
#include <boost/algorithm/string/regex.hpp>

#include <experimental/filesystem>

using namespace std;
using namespace boost;

bool flag = false;
vector <int> clientsocks;  //for storing the returned file descriptors of accepted sockets
vector <string> recipients; //for storing the accepted users in the mailing list
void *worker(void*);
string BACKEND_SERVER = "127.0.0.1:50051"; //the port for backend master
KVSClientInterface bigTableClient;

void termination_handler(int signum) //for terminating using Ctrl + C
{
    for(unsigned int i=0; i < clientsocks.size(); i++)
    {
        string m= "-ERR Server shutting down\n";
        send(clientsocks[i], m.c_str(), strlen(m.c_str()), 0);
        close(clientsocks[i]);
    }
    exit(1); 

}  

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


int main(int argc, char *argv[])
{

    int port = 2500; //default port decleration
    int option, reuse = 1;
    int server, client;

    //creating a new socket
    int sockfd = socket(PF_INET, SOCK_STREAM, 0); //returns a file descriptor to the new socket
    if (sockfd < 0) 
        {  
             cerr<< "error creating socket" << endl;
             exit(1);
        }  
    
    //command line input 
    while ((option = getopt(argc,argv,"p:av")) != -1) 
    {
        switch (option) {
            
            case 'p':
                port = atoi(optarg);
                break;
            
            case 'a':
                cerr<< "Shivendra Pandey / pshiv" <<endl;
                exit(0);
                break;

            case 'v':
                flag = true;
                break;

            default:
                exit(0);
        }
    }

    //connect to the server
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr)); //Sets the first sizeof(servaddr) block of memory pointed by ptr(&servaddr) to the specified value(2nd parameter) 
    servaddr.sin_family = AF_INET; //AF(address family) and PF(protocol family) are coupled, related
    servaddr.sin_port = htons(port); //listening on a particular port
    servaddr.sin_addr.s_addr = INADDR_ANY; 
    bind(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr)); //binding the TCP socket to the port no. and the current machine's IP address
      
      // set socket to immediately reuse port when the application closes
    if(setsockopt(sockfd,SOL_SOCKET, (SO_REUSEPORT | SO_REUSEADDR), (char*)&reuse, sizeof(reuse)) < 0)
        {
            cout<< "setsockopt failed" << endl;
            close(sockfd);
            exit(2);
        }
      
    bigTableClient = KVSClientInterface(BACKEND_SERVER); //constructor for connecting to the table

      // putting the socket into listening state for incoming connections
    if (listen(sockfd, 10) < 0) 
    {
        cerr<< "Not Able to listen" << endl;
        exit(0);
    }   
    //accepting the clients on a particular socket
    struct sockaddr_in clientaddr;
    socklen_t clientaddrlen = sizeof(clientaddr);
    int comm_fd[1000];

    while ((*comm_fd = accept(sockfd, (struct sockaddr*)&clientaddr, &clientaddrlen)) > 0) 
    {
        string s = "220 localhost service ready\r\n"; 

        if (flag == true) //printing the debug output to cerr
        {
            cerr << "[" << *comm_fd << "] S: " << s <<  endl;
        }
        send(*comm_fd, s.c_str(), strlen(s.c_str()), 0); //server to client
        pthread_t thread;
        pthread_create(&thread, NULL, worker, comm_fd);

        clientsocks.push_back(*comm_fd); //storing the returned file descriptors of accepted sockets in vector clientsocks
        //using sigaction for handling Ctrl + C condition
        struct sigaction Handler;
        sigemptyset(&Handler.sa_mask);// no signal is blocked while the handler run.
        Handler.sa_flags = 0;
        Handler.sa_handler = termination_handler; //setting the handler in new 'Handler' struct      
        sigaction(SIGINT, &Handler, NULL); // calling the handler
             
    }
    
    close(sockfd);
}

void *worker (void *arg)
{
        int buflen, nread, i=0;
        int seq = 0; //for checking the sequence of commands
        int comm_fd = *(int*) arg;
        char c;
        bool helo = false, dataflag = false; //true if helo has been executed
        // allocate buffer for reading data from socket
        buflen = 1; //for reading 1 character at a time
        char *buf= new char[1000];
        char *newbuf = new char[buflen+1];
        char *data = new char[10000]; 
        char *revpath = new char[256];
        vector <string> fwd_path;

        //reading commands from the connection
        while(true)
        {           
            string cmd; //for storing each command 
            //reading character by character from the client 
            while(true)
            {
                nread = recv(comm_fd,newbuf,buflen,0); //client to server, execution stops here until client sends some data
                if(dataflag == true)
                {       
                        const char* dat = cmd.c_str();
                        if(strstr(dat, "\r\n.\r") != NULL) // finding the period for the termination of the mail body
                        {
                            break;
                        }
                    
                }
                else if(newbuf[0] == '\n')
                {
                    break;
                }    
                cmd +=newbuf[0]; 
            }   

            if (flag == true) //printing the debug output to cerr
            {
                cerr << "[" << comm_fd << "] C: " <<  cmd << endl;
            }

            strcpy(buf, cmd.c_str()); //copying the content of cmd to char pointer buf

            if (nread == 0)
            break;

            string str(buf);

            // HELO command condition
            if(strcasecmp ((const char*)str.substr(0,4).c_str(), "helo") == 0) 
            {
                
                    if(seq == 0 || seq == 1)
                    {                   
                        //checking if helo has an argument or not
                        size_t pos = str.find(" ");
                        size_t end = str.find("\r");
                        int len = end - pos - 1;
                        string hel_arg = str.substr(pos + 1, len);
                        if (pos == -1 || hel_arg == "")
                        {
                            string err= "501 Syntax error in parameters or arguments\n";
                            if (flag == true) //printing the debug output to cerr
                            {
                                 cerr << "[" << comm_fd << "] S: " <<  err << endl;
                            }
                            send(comm_fd , err.c_str(), strlen(err.c_str()), 0);
                        }
                        else
                        {
                            seq = 1;
                            string s = "250 localhost\r\n";
                            if (flag == true) //printing the debug output to cerr
                            {
                                 cerr << "[" << comm_fd << "] S: " <<  s << endl;
                            }
                            send(comm_fd , s.c_str(), strlen(s.c_str()), 0); //server to client
                        }
                    }
                    else
                    {
                        string err = "503 Bad sequence of commands\r\n";
                        if (flag == true) //printing the debug output to cerr
                        {
                             cerr << "[" << comm_fd << "] S: " <<  err << endl;
                        }
                        send(comm_fd , err.c_str(), strlen(err.c_str()), 0);

                    }
            }
            else if(strcasecmp ((const char*)str.substr(0,10).c_str(), "mail from:") == 0) // MAIL FROM: < > case
            {
                
                    if (seq == 1) //checking if 'mail from' is implemented after HELO command
                    {
                        size_t pos = str.find('@');
                        if(pos !=-1) // Sanity Check
                        {
                            seq = 2;
                            fwd_path.clear(); //clearing the forward path buffer
                            memset (data, 0,sizeof(data)); //clearing the mail data buffer
                            memset (revpath, 0, sizeof(revpath)); //clearing the reverse path buffer
                            size_t st = str.find("<");
                            size_t end = str.find(">");
                            int len = end - st - 1;
                            string reverse = str.substr(st + 1, len);
                            strcpy(revpath, reverse.c_str()); //copying the reverse path into reverse path buffer
                            string s = "250 OK\r\n";
                            if (flag == true) //printing the debug output to cerr
                            {
                                 cerr << "[" << comm_fd << "] S: " <<  s << endl;
                            }
                            send(comm_fd, s.c_str(), strlen(s.c_str()), 0); //server to client
                        }
                        else
                        {
                            string err = "501 Syntax error in parameters or arguments\r\n";
                            if (flag == true) //printing the debug output to cerr
                            {
                                 cerr << "[" << comm_fd << "] S: " <<  err << endl;
                            }
                            send(comm_fd , err.c_str(), strlen(err.c_str()), 0);

                        }
                    }
                    else //if sequence of commands is broken
                    {
                        string err = "503 Bad sequence of commands\r\n";
                        if (flag == true) //printing the debug output to cerr
                        {
                             cerr << "[" << comm_fd << "] S: " <<  err << endl;
                        }
                        send(comm_fd , err.c_str(), strlen(err.c_str()), 0);
                    }
             }  
             //RCPT TO:< > command case
             else if(strcasecmp ((const char*)str.substr(0,8).c_str(), "rcpt to:") == 0) //RCPT TO: < > case
             {
                if(seq == 2 || seq == 3) //rcpt to: < > can be called form previous as well as present state
                {
                    seq = 3;
                    bool rec = false;
                    size_t st = str.find("<");
                    size_t end = str.find(">");
                    int len = end - st - 1;
                    string forward = str.substr(st + 1, len);
                    size_t atPos = forward.find('@');
                    string domain = forward.substr(atPos + 1);
                    if(atPos == string::npos){
                        string err = "Bad formation of recipient\r\n";
                        send(comm_fd , err.c_str(), strlen(err.c_str()), 0);
                    }
                    else if(strcasecmp(domain.c_str(), "localhost.com") == 0){
                        string recipient = forward.substr(0, atPos);
                        string value =  bigTableClient.get(recipient, "password"); //checking if the user exists in the bigTable
                        if(strcasecmp(value.c_str(), "0") == 0){
                            string err = "550 No such user here\r\n";
                            send(comm_fd , err.c_str(), strlen(err.c_str()), 0);
                        }
                        else{
                            string s = "250 OK\r\n";
                            send(comm_fd , s.c_str(), strlen(s.c_str()), 0);
                            fwd_path.push_back(forward);
                        }
                    }
                    else{
                        string s = "250 OK\r\n";
                        send(comm_fd , s.c_str(), strlen(s.c_str()), 0);
                        fwd_path.push_back(forward);
                    }
                    for(unsigned int i = 0; i<fwd_path.size() ; i++){
                        cout << fwd_path[i] << endl;
                    }
                }
                else
                {
                    string err = "503 Bad sequence of commands\r\n";
                    if (flag == true) //printing the debug output to cerr
                    {
                         cerr << "[" << comm_fd << "] S: " <<  err << endl;
                    }
                    send(comm_fd , err.c_str(), strlen(err.c_str()), 0);

                }
             }
             else if(strcasecmp ((const char*)str.substr(0,4).c_str(), "data") == 0)
             {
                if(seq == 3)
                {
                    dataflag = true;
                    string s = "354 Start mail input; end with <CRLF>.<CRLF>\r\n" ;
                    if (flag == true) //printing the debug output to cerr
                    {
                         cerr << "[" << comm_fd << "] S: " <<  s << endl;
                    }   
                    send(comm_fd , s.c_str(), strlen(s.c_str()), 0);    
                }
                else
                {
                    string err = "503 Bad sequence of commands\r\n";
                    if (flag == true) //printing the debug output to cerr
                    {
                         cerr << "[" << comm_fd << "] S: " <<  err << endl;
                    }
                    send(comm_fd , err.c_str(), strlen(err.c_str()), 0);
                }

             }    
             else if( dataflag == true)
             {
                 cmd.pop_back(); //eliminating the final dot from the mail body
                 cmd.pop_back();
                 if(200000 < strlen(cmd.c_str()) ) //checking if storage allocation is less than the mail body size
                 {
                    string err = "552 Requested mail action aborted: exceeded storage allocation \r\n";
                    if (flag == true) //printing the debug output to cerr
                    {
                         cerr << "[" << comm_fd << "] S: " <<  err << endl;
                    }
                    send(comm_fd , err.c_str(), strlen(err.c_str()), 0);
                    break;
                 }
                 strcpy (data, cmd.c_str());
                 seq = 4;
                 dataflag = false;

                 Json::FastWriter writer; //converts json object to string
                 Json::Reader reader;     //converts string to json object
                 //starting to send 
                 for(unsigned int i=0 ; i < fwd_path.size() ; i++)
                 {
                    size_t atPos = fwd_path[i].find('@');
                    string domain = fwd_path[i].substr(atPos + 1);
                    string recipient = fwd_path[i].substr(0, atPos);

                    Json::Value mailContent;
                    mailContent["to"] = algorithm::join(fwd_path, ",");
                    mailContent["from"] = revpath;
                    mailContent["subject"] = "";
                    mailContent["date"] = getTimeFromNow(0,0,0);
                    mailContent["message"] = data;
                    string mailContentStr = writer.write(mailContent);
                    if(strcasecmp(domain.c_str(), "localhost.com") == 0){                           
                        string mailContentKeyStr = to_string(time(NULL)); // key for mail being k1, k2 and so on
                        string response = bigTableClient.put(recipient, mailContentKeyStr, mailContentStr);
                        if(strcasecmp(response.c_str(), "RPC failed") == 0){
                        string err = "RPC has failed\r\n";
                        send(comm_fd , err.c_str(), strlen(err.c_str()), 0);
                        }
                        string inboxContentStr = bigTableClient.get(recipient, "inbox"); //getting the inbox content for the particular recipient
                        cout << recipient << " inbox cont: " << inboxContentStr << endl;
                        Json::Value inboxContent/*(Json::arrayValue)*/;
                        reader.parse(inboxContentStr.c_str(), inboxContent); //parsing inboxConetntStr and appending to inboxContent
                        inboxContent.append(mailContentKeyStr);
                        inboxContentStr = writer.write(inboxContent);
                        response = bigTableClient.put(recipient, "inbox", inboxContentStr);
                        if(strcasecmp(response.c_str(), "RPC failed") == 0){
                        string err = "RPC has failed\r\n";
                        send(comm_fd , err.c_str(), strlen(err.c_str()), 0);
                        }
                    }
                    else{
                        MailService mailService(revpath, fwd_path[i], data);
                        int response = mailService.sendMail();
                        cout << response << endl;
                    }
                }

                 // clearing the buffers on successful processing
                 fwd_path.clear(); //clearing the forward path buffer
                 memset (data, 0,sizeof(data)); //clearing the mail data buffer
                 memset (revpath, 0, sizeof(revpath)); //clearing the reverse path buffer

                string s = "250 OK\r\n" ;
                if (flag == true) //printing the debug output to cerr
                {
                     cerr << "[" << comm_fd << "] S: " <<  s << endl;
                }               
                send(comm_fd , s.c_str(), strlen(s.c_str()), 0);                 
             }
             //QUIT command case
             else if(strcasecmp ((const char*)str.substr(0,4).c_str(), "quit") == 0 )
             {
                    string s = "221 localhost Service closing transmission channel\r\n" ;
                    if (flag == true) //printing the debug output to cerr
                    {
                         cerr << "[" << comm_fd << "] S: " <<  s << endl;
                    }
                    send(comm_fd , s.c_str(), strlen(s.c_str()), 0);
                    //closing the transmission channel
                    close (comm_fd);
                    pthread_exit(NULL);
             }
             //NOOP command case
             else if(strcasecmp ((const char*)str.substr(0,4).c_str(), "noop") == 0 )
             {
                string s = "250 OK\r\n" ;
                if (flag == true) //printing the debug output to cerr
                 {
                    cerr << "[" << comm_fd << "] S: " <<  s << endl;
                 }
                send(comm_fd , s.c_str(), strlen(s.c_str()), 0);

             }
             //RSET command case
             else if(strcasecmp ((const char*)str.substr(0,4).c_str(), "rset") == 0 )
             {
                //clear all buffers and execute the mode after "HELO"
                fwd_path.clear(); //clearing the forward path vector
                memset (data, 0,sizeof(data)); //clearing the mail data buffer
                memset (revpath, 0, sizeof(revpath)); //clearing the reverse path buffer
                seq = 1; //restoring the state after 'helo'
                string s = "250 OK\r\n" ;
                if (flag == true) //printing the debug output to cerr
                {
                    cerr << "[" << comm_fd << "] S: " <<  s << endl;
                }
                send(comm_fd , s.c_str(), strlen(s.c_str()), 0);
             }
             else
             {
                string err = "500 Syntax error, command unrecognized\r\n" ;
                if (flag == true) //printing the debug output to cerr
                 {
                    cerr << "[" << comm_fd << "] S: " <<  err << endl;
                 }
                send(comm_fd , err.c_str(), strlen(err.c_str()), 0);
             }        
        }
        close (comm_fd);
        pthread_exit(NULL);
}