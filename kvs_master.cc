#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <stdio.h>
#include <ctype.h>

#include <grpc++/grpc++.h>
#include "kvs_master.grpc.pb.h"

// Implementation of the KVS service. 
// All of the service's methods need to be implemented.
// These will be invoked by the running gRPC server.
class KVS_masterImpl final : public kvs_master::KVS_master_service::Service
{
public:

  // Called by front end server.
  grpc::Status get_leader(grpc::ServerContext* context,
                        const kvs_master::GetLeaderValueRequest* request,
                        kvs_master::GetLeaderValueReply* reply) override {
    std::lock_guard<std::mutex> lock(db_mutex_);

    std::string key = request->user();
    char k = key.at(0);
    k = tolower(k);

    if(k >= 97 && k <= 106)
    {
      auto it = string_db_.find("a");
      reply->set_url(it->second);
    }
    else if(k >= 107 && k <= 116)
    {
      auto it = string_db_.find("k");
      reply->set_url(it->second);
    }
    else
    {
      auto it = string_db_.find("u");
      reply->set_url(it->second);
    }

    return grpc::Status::OK;
  }

  // Called by backend cluster master, to request master addr.
  grpc::Status send_addr(grpc::ServerContext* context,
                      const kvs_master::SendAddrValueRequest* request,
                      kvs_master::SendAddrValueReply* reply) override {

    std::string address = "0.0.0.0:50051";
    reply->set_addr(address);

    return grpc::Status::OK;
  }

  // Called by backend cluster master, to specify cluster master.
  grpc::Status set_leader(grpc::ServerContext* context,
                        const kvs_master::SetLeaderValueRequest* request,
                        kvs_master::SetLeaderValueReply* reply) override {
    std::lock_guard<std::mutex> lock(db_mutex_);

    std::string key = request->key();

    string_db_[key] = request->url();
    reply->set_ret("1");

    return grpc::Status::OK;
  }

private:
  // The actual database.
  std::map<std::string, std::string> string_db_;

  // Mutex serializing access to the map.
  std::mutex db_mutex_;
};

void RunServer()
{
  // Server listens on this address.
  std::string server_address("0.0.0.0:50051");
  KVS_masterImpl service;

  grpc::ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with clients.
  // In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv)
{
  RunServer();

  //std::thread thrd(&RunServer);

  //st::cout << "Hi" << std::endl;

  //thrd.join();

  return 0;
}