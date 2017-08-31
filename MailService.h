#ifndef MAIL_SERVICE_H
#define MAIL_SERVICE_H

#include <iostream>
#include <map>
#include <algorithm>
#include <fstream>
#include <vector>
#include <string>

#include <experimental/filesystem>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/utsname.h>
#include <netinet/in.h>

#include <netdb.h>
#include <arpa/nameser.h>
#include <arpa/inet.h>
#include <resolv.h>

using namespace std;

class MailService{
    public:

        string recvFrom, rcptTo, data;

        MailService();

        MailService(string recvFrom, string rcptTo, string data);

        int sendMail();

    private:

        map<int, vector<string>> getMXRecords(string host);

        string sendCommand(int sockfd, string command, int expectedResponseCode);

        int _sendMail(string host, int portNo, string mailFrom, string rcptTo, string data);

};

#endif