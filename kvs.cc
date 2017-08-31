#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include <grpc++/grpc++.h>
#include "kvs.grpc.pb.h"

// Implementation of the KVS service. 
// All of the service's methods need to be implemented.
// These will be invoked by the running gRPC server.
class KVSImpl final : public kvs::KVS_service::Service{
    public:
        grpc::Status get(grpc::ServerContext* context, const kvs::GetValueRequest* request, kvs::GetValueReply* reply) override {
            std::lock_guard<std::mutex> lock(db_mutex_);
            std::string key = request->row() + "," + request->col();
            auto it = string_db_.find(key);
            std::cout << "printing the map" << std::endl;
            for(auto it = string_db_.begin() ; it != string_db_.end() ; it ++){
                std::cout << it->first << "    " << it->second << std::endl;
            }
            std::cout << "Key is: " << key << std::endl;
            if(it == string_db_.end()){
                reply->set_value("0");
            }
            else{
                reply->set_value(it->second);
            }
            return grpc::Status::OK;
        }

        grpc::Status put(grpc::ServerContext* context, const kvs::PutValueRequest* request, kvs::PutValueReply* reply) override {
            std::lock_guard<std::mutex> lock(db_mutex_);
            std::string key = request->row() + "," + request->col();
            string_db_[key] = request->value();
            reply->set_value("1");
            return grpc::Status::OK;
        }

        grpc::Status cput(grpc::ServerContext* context, const kvs::CPutValueRequest* request, kvs::CPutValueReply* reply) override {
            std::lock_guard<std::mutex> lock(db_mutex_);
            std::string key = request->row() + "," + request->col();
            auto it = string_db_.find(key);
            if(it == string_db_.end()){
                reply->set_value("0");
            }
            else{
                if(strcasecmp((it->second).c_str(), (request->value1()).c_str())==0){
                    it->second = request->value2();
                    reply->set_value("1");
                }
                else{
                    reply->set_value("0");
                }
            }
            return grpc::Status::OK;
        }

        grpc::Status del(grpc::ServerContext* context, const kvs::DeleteValueRequest* request, kvs::DeleteValueReply* reply) override {
            std::lock_guard<std::mutex> lock(db_mutex_);
            std::string key = request->row() + "," + request->col();
            string_db_.erase(key);
            reply->set_value("1");
            return grpc::Status::OK;
        }

    private:
        // The actual database.
        std::map<std::string, std::string> string_db_;

        // Mutex serializing access to the map.
        std::mutex db_mutex_;
};

void RunServer(){
    // Server listens on this address.
    std::string server_address("127.0.0.1:50051");
    KVSImpl service;

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

int main(int argc, char** argv){
    RunServer();
    return 0;
}