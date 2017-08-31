#ifndef MAIL_SERVICE_H

#include "MailService.h"

#endif

using namespace std;

MailService::MailService(string recvFrom, string rcptTo, string data){
    this->recvFrom = recvFrom;
    this->rcptTo = rcptTo;
    this->data = data;
}

map<int, vector<string>> MailService::getMXRecords(string host){
    int LENGTH = 4096;
    unsigned char answer[LENGTH + 1];
    ns_msg msg;
    int cnt_an;
    ns_rr rr;
    char buffer[LENGTH + 1];

    map<int, vector<string>> mxRecords;
    
    int resLen = 0;
    if((resLen = res_query(host.c_str(), ns_c_in, ns_t_mx, answer, LENGTH)) < 0){
        herror("res_query error: ");
        return mxRecords;
    }
    answer[resLen] = '\0';
    fprintf(stdout, "Response Length: %d\n", resLen);
    if(ns_initparse(answer, resLen, &msg) < 0){
        herror("parse error: ");
        return mxRecords;
    }
    if((cnt_an = ns_msg_count(msg, ns_s_an)) <= 0){
        herror("count error: ");
        return mxRecords;
    }
    for(int i = 0 ; i < cnt_an ; i ++){
        if(ns_parserr(&msg, ns_s_an, i, &rr) < 0){
            herror("parserr error: ");
            return mxRecords;
        }
        ns_sprintrr(&msg, &rr, NULL, NULL, buffer, LENGTH);
        
        const u_char *rdata = ns_rr_rdata(rr);
        char exchange[4096];
        int priority = ns_get16(rdata);
        int len = dn_expand(answer, answer + resLen, rdata + 2, exchange, sizeof(exchange));
        fprintf(stdout, "Priority: %d\n", priority);
        fprintf(stdout, "Exchange: %s\n", exchange);    
        if(mxRecords.find(priority) == mxRecords.end()){
            mxRecords[priority] = {};
        }
        mxRecords[priority].push_back(exchange);
    }
    return mxRecords;
}

string MailService::sendCommand(int sockfd, string command, int expectedResponseCode){
    int LENGTH = 4096;
    char buffer[LENGTH + 1];

    if(send(sockfd, command.c_str(), command.size(), 0) < 0){
        perror("send error (Could not send mail from command): ");
        return "";
    }
    fprintf(stderr, "ME: %s\n", command.c_str());
    int resLen = 0;
    if((resLen = recv(sockfd, buffer, LENGTH, 0)) < 0){
        perror("recv error (Could not recv the reply for mail from command): ");
        return "";
    }
    buffer[resLen] = '\0';
    fprintf(stderr, "THEY: %s\n", buffer);
    if(atoi(buffer) != expectedResponseCode){
        return "";
    }
    string retValue = buffer;
    return retValue;
}

int MailService::_sendMail(string host, int portNo, string mailFrom, string rcptTo, string data){
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    int rv = 0;
    fprintf(stderr, "host is %s\n", host.c_str());
    if((rv = getaddrinfo(host.c_str(), to_string(portNo).c_str(), &hints, &res)) != 0){
        fprintf(stderr, "Error! Return value: %d\n%s\n", rv, gai_strerror(rv));
        return 1;
    }
    else{
        fprintf(stderr, "Connected to: %s:%s\n", host.c_str(), to_string(portNo).c_str());
    }

    int sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if(sockfd < 0){
        perror("Socket Error: ");
        return 1;
    }
    else{
        fprintf(stderr, "Socket created\n");
    }

    if(connect(sockfd, res->ai_addr, res->ai_addrlen) != 0){
        perror("connect error: ");
        return 1;
    }
    else{
        fprintf(stderr, "Connection created\n");
        int LENGTH = 4096;
        char buffer[LENGTH + 1];
        int resLen = 0;
        if((resLen = recv(sockfd, buffer, LENGTH, 0)) < 0){
            perror("recv error (Could not recv the reply for connection): ");
            return 1;
        }
        buffer[resLen] = '\0';
        fprintf(stderr, "THEY: %s\n", buffer);
    }
    
    string command = "HELO localhost\r\n";
    sendCommand(sockfd, command, 250);

    command = "MAIL FROM:" + mailFrom + "\r\n";
    if(sendCommand(sockfd, command, 250).size() == 0){
        return 1;
    }

    command = "RCPT TO:" + rcptTo + "\r\n";
    if(sendCommand(sockfd, command, 250).size() == 0){
        return 1;
    }

    command = "DATA\r\n";
    if(sendCommand(sockfd, command, 354).size() == 0){
        return 1;
    }

    command = data + "\r\n.\r\n";
    if(sendCommand(sockfd, command, 250).size() == 0){
        return 1;
    }

    close(sockfd);

    return 0;
}

int MailService::sendMail(){
    string recipient = this->rcptTo;
    int ATPos = recipient.find("@");
    int endAnglePos = recipient.find(">");
    string host = recipient.substr(ATPos + 1, endAnglePos - ATPos - 1);
    fprintf(stderr, "Processing host: %s\n", host.c_str());
    map<int, vector<string>> mxRecords = getMXRecords(host);
    for(map<int, vector<string>>::iterator it = mxRecords.begin() ; it != mxRecords.end() ; it ++){
        fprintf(stderr, "With Priority: %d\n", it->first);
        for(vector<string>::iterator exchange_it = it->second.begin() ; exchange_it != it->second.end() ; exchange_it ++){
            fprintf(stderr, "Trying Exchange: %s\n", (*exchange_it).c_str());
            string smtpServer = *exchange_it;
            int portNo = 25;
            int result = _sendMail(smtpServer, portNo, this->recvFrom, this->rcptTo, this->data);
            if(result == 0){
                // mail sent successfully
                return 0;
            }
        }
    }
    return 1;
}