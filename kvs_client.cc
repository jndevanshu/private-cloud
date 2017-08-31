#include <iostream>
#include <memory>
#include <string>

#include <grpc++/grpc++.h>
#include "kvs_master.grpc.pb.h"
#include "kvs_cluster.grpc.pb.h"

using grpc::Channel;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ClientContext;

using kvs_master::GetLeaderValueRequest;
using kvs_master::GetLeaderValueReply;
using kvs_master::SendAddrValueRequest;
using kvs_master::SendAddrValueReply;
using kvs_master::SetLeaderValueRequest;
using kvs_master::SetLeaderValueReply;
using kvs_master::KVS_master_service;

using kvs_cluster::PutValueRequest;
using kvs_cluster::PutValueReply;
using kvs_cluster::CPutValueRequest;
using kvs_cluster::CPutValueReply;
using kvs_cluster::GetValueRequest;
using kvs_cluster::GetValueReply;
using kvs_cluster::DeleteValueRequest;
using kvs_cluster::DeleteValueReply;
using kvs_cluster::SendSelfAddrRequest;
using kvs_cluster::SendSelfAddrReply;
using kvs_cluster::GetMasterInitRequest;
using kvs_cluster::GetMasterInitReply;
using kvs_cluster::GetUpdateRequest;
using kvs_cluster::GetUpdateReply;
using kvs_cluster::KVS_cluster_service;

class KVSClient
{
    public:
        KVSClient(std::shared_ptr<Channel> channel): stub_(KVS_cluster_service::NewStub(channel)) {}

        // Assembles the client's payload, sends it and presents the response back
        // from the server.
        std::string put(const std::string& row, const std::string& col, std::string& val){
            // Data we are sending to the server.
            PutValueRequest request;
            request.set_row(row);
            request.set_col(col);
            request.set_value(val);

            // Container for the data we expect from the server.
            PutValueReply reply;

            // Context for the client. It could be used to convey extra information to
            // the server and/or tweak certain RPC behaviors.
            ClientContext context;

            // The actual RPC.
            Status status = stub_->put(&context, request, &reply);

            // Act upon its status.
            if (status.ok()){
                return reply.value();
            }
            else{
                std::cout << status.error_code() << ": " << status.error_message() << std::endl;
                return "RPC failed";
            }
        }

  std::string cput(const std::string& row, const std::string& col, std::string& val1, std::string& val2)
  {
    // Data we are sending to the server.
    CPutValueRequest request;
    request.set_row(row);
    request.set_col(col);
    request.set_value1(val1);
    request.set_value2(val2);

    // Container for the data we expect from the server.
    CPutValueReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->cput(&context, request, &reply);

    // Act upon its status.
    if (status.ok())
    {
      return reply.value();
    }
    else
    {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

  std::string get(const std::string& row, const std::string& col)
  {
    // Data we are sending to the server.
    GetValueRequest request;
    request.set_row(row);
    request.set_col(col);

    // Container for the data we expect from the server.
    GetValueReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->get(&context, request, &reply);

    // Act upon its status.
    if (status.ok())
    {
      return reply.value();
    }
    else
    {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

  std::string del(const std::string& row, const std::string& col)
  {
    // Data we are sending to the server.
    DeleteValueRequest request;
    request.set_row(row);
    request.set_col(col);

    // Container for the data we expect from the server.
    DeleteValueReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    // The actual RPC.
    Status status = stub_->del(&context, request, &reply);

    // Act upon its status.
    if (status.ok())
    {
      return reply.value();
    }
    else
    {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

 private:
  std::unique_ptr<KVS_cluster_service::Stub> stub_;
};

class KVSCluster1_master
{
 public:
  KVSCluster1_master(std::shared_ptr<Channel> channel)
      : stub_(KVS_master_service::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string get_leader(const std::string& user)
  {
    // Data we are sending to the server.
    GetLeaderValueRequest request;
    request.set_user(user);

    // Container for the data we expect from the server.
    GetLeaderValueReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    gpr_timespec timeOut;

    timeOut.tv_sec = 15;
    timeOut.tv_nsec = 0;
    timeOut.clock_type = GPR_TIMESPAN;
    context.set_deadline(timeOut);

    // The actual RPC.
    Status status = stub_->get_leader(&context, request, &reply);

    if (!status.ok() && status.error_code() == grpc::DEADLINE_EXCEEDED)
    {
        // call timeout handler
    }

    // Act upon its status.
    if (status.ok())
    {
      //std::cout << reply.url() << std::endl;
      return reply.url();
    }
    else
    {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

  std::string send_addr()
  {
    // Data we are sending to the server.
    SendAddrValueRequest request;

    // Container for the data we expect from the server.
    SendAddrValueReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    gpr_timespec timeOut;

    timeOut.tv_sec = 15;
    timeOut.tv_nsec = 0;
    timeOut.clock_type = GPR_TIMESPAN;
    context.set_deadline(timeOut);

    // The actual RPC.
    Status status = stub_->send_addr(&context, request, &reply);

    if (!status.ok() && status.error_code() == grpc::DEADLINE_EXCEEDED)
    {
        // call timeout handler
    }

    // Act upon its status.
    if (status.ok())
    {
      //std::cout << reply.addr() << std::endl;
      return reply.addr();
    }
    else
    {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

  std::string set_leader(const std::string& key, const std::string& url)
  {
    // Data we are sending to the server.
    SetLeaderValueRequest request;
    request.set_key(key);
    request.set_url(url);

    // Container for the data we expect from the server.
    SetLeaderValueReply reply;

    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;

    gpr_timespec timeOut;

    timeOut.tv_sec = 15;
    timeOut.tv_nsec = 0;
    timeOut.clock_type = GPR_TIMESPAN;
    context.set_deadline(timeOut);

    // The actual RPC.
    Status status = stub_->set_leader(&context, request, &reply);

    if (!status.ok() && status.error_code() == grpc::DEADLINE_EXCEEDED)
    {
        // call timeout handler
    }

    // Act upon its status.
    if (status.ok())
    {
      //std::cout << reply.ret() << std::endl;
      return reply.ret();
    }
    else
    {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

 private:
  std::unique_ptr<KVS_master_service::Stub> stub_;
};

class KVSClientInterface{

    public:
        std::string backendServer;

        KVSClientInterface(){}

        KVSClientInterface(std::string server){
            backendServer = server;
        }

        KVSClient getClusterPrimary(std::string user){
            // Instantiate the client. It requires a channel, out of which the actual RPCs
            // are created. This channel models a connection to an endpoint (in this case,
            // localhost at port 50051). We indicate that the channel isn't authenticated
            // (use of InsecureChannelCredentials()).

            KVSCluster1_master objz(grpc::CreateChannel(backendServer, grpc::InsecureChannelCredentials()));
            std::cout << objz.get_leader(user) << std::endl;
            std::string cluster_addr = objz.get_leader(user);
            KVSClient obj(grpc::CreateChannel(cluster_addr, grpc::InsecureChannelCredentials()));

            return obj;
        }

        std::string put(const std::string& row, const std::string& col, std::string& val){
            KVSClient obj = getClusterPrimary(row);
            std::string response = obj.put(row, col, val);
            std::cout << "\n\n\n" << response << "\n\n\n" << std::endl;
            return response;
        }

        std::string cput(const std::string& row, const std::string& col, std::string& val1, std::string& val2){
            KVSClient obj = getClusterPrimary(row);
            std::string response = obj.cput(row, col, val1, val2);
            return response;
        }

        std::string get(const std::string& row, const std::string& col){
            KVSClient obj = getClusterPrimary(row);
            std::string response = obj.get(row, col);
            std::cout << "\n\n\n" << response << "\n\n\n" << std::endl;
            return response;
        }

        std::string del(const std::string& row, const std::string& col){
            KVSClient obj = getClusterPrimary(row);
            std::string response = obj.del(row, col);
            return response;
        }

};