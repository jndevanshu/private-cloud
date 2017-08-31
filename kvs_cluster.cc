#include <unistd.h>

#include <sys/types.h>
#include <signal.h>

#include <sstream>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <chrono>         // std::chrono::seconds

#include <grpc++/grpc++.h>
#include "kvs_master.grpc.pb.h"
#include "kvs_cluster.grpc.pb.h"

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/regex.hpp>
#include <boost/algorithm/string/regex.hpp>

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
using kvs_cluster::DieRequest;
using kvs_cluster::DieReply;
using kvs_cluster::KVS_cluster_service;

using namespace boost;

//Global var
std::string oa;   //own_addr
int im;   //is_mstr

// The actual database.
std::map<std::string, std::string> string_db_;
std::string k;
std::string v;

// Bookkeeping
// Master addr, Self addr, Cluster Master, N1 addr, N2 addr
class Booker
{
public:
  void set_masterAddr(std::string addr)
  {
    string_db_bk_["master_addr"] = addr;
  }

  std::string get_masterAddr()
  {
    return string_db_bk_["master_addr"];
  }

  void set_selfAddr(std::string addr)
  {
    string_db_bk_["self_addr"] = addr;
  }

  std::string get_selfAddr()
  {
    return string_db_bk_["self_addr"];
  }

  void set_N1Addr(std::string addr)
  {
    string_db_bk_["n1_addr"] = addr;
  }

  std::string get_N1Addr()
  {
    return string_db_bk_["n1_addr"];
  }

  void set_N2Addr(std::string addr)
  {
    string_db_bk_["n2_addr"] = addr;
  }

  std::string get_N2Addr()
  {
    return string_db_bk_["n2_addr"];
  }

  void set_ClusterMaster(std::string addr)
  {
    string_db_bk_["clstr_mstr"] = addr;
  }

  std::string get_ClusterMaster()
  {
    return string_db_bk_["clstr_mstr"];
  }  

private:
  // Bookkeeping database.
  std::map<std::string, std::string> string_db_bk_;
};

// Implementation of the KVS service. 
// All of the service's methods need to be implemented.
// These will be invoked by the running gRPC server.
class KVS_clusterImpl final : public kvs_cluster::KVS_cluster_service::Service
{
public:
  grpc::Status selfAddr(grpc::ServerContext* context,
                        const kvs_cluster::SendSelfAddrRequest* request,
                        kvs_cluster::SendSelfAddrReply* reply) override {

    
    //reply->set_addr("0.0.0.0:60051");
    reply->set_addr(oa);

    return grpc::Status::OK;
  }

  grpc::Status clusterMaster(grpc::ServerContext* context,
                        const kvs_cluster::GetMasterInitRequest* request,
                        kvs_cluster::GetMasterInitReply* reply) override {

    if(im)
    {
      reply->set_ret("yes");
    }
    else
    {
      reply->set_ret("no");
    }

    return grpc::Status::OK;
  }

// KVS Primitives
  grpc::Status get(grpc::ServerContext* context,
                        const kvs_cluster::GetValueRequest* request,
                        kvs_cluster::GetValueReply* reply) override {
    std::lock_guard<std::mutex> lock(db_mutex_);

    std::string key = request->row() + "," + request->col();

    auto it = string_db_.find(key);
    if(it == string_db_.end())
    {
      reply->set_value("0");
    }
    else
    {
      reply->set_value(it->second);
    }

    return grpc::Status::OK;
  }

  grpc::Status put(grpc::ServerContext* context,
                        const kvs_cluster::PutValueRequest* request,
                        kvs_cluster::PutValueReply* reply) override {
    std::lock_guard<std::mutex> lock(db_mutex_);

    std::string key = request->row() + "," + request->col();

    string_db_[key] = request->value();
    reply->set_value("1");

    return grpc::Status::OK;
  }

  grpc::Status cput(grpc::ServerContext* context,
                        const kvs_cluster::CPutValueRequest* request,
                        kvs_cluster::CPutValueReply* reply) override {
    std::lock_guard<std::mutex> lock(db_mutex_);

    std::string key = request->row() + "," + request->col();

    auto it = string_db_.find(key);
    if(it == string_db_.end())
    {
      reply->set_value("0");
    }
    else
    {
      if(strcasecmp((it->second).c_str(), (request->value1()).c_str())==0)
      {
        it->second = request->value2();
        reply->set_value("1");
      }
      else
      {
        reply->set_value("0");
      }
    }
    
    return grpc::Status::OK;
  }

  grpc::Status del(grpc::ServerContext* context,
                          const kvs_cluster::DeleteValueRequest* request,
                          kvs_cluster::DeleteValueReply* reply) override {
    std::lock_guard<std::mutex> lock(db_mutex_);

    std::string key = request->row() + "," + request->col();

    string_db_.erase(key);
    reply->set_value("1");

    return grpc::Status::OK;
  }

  grpc::Status get_update(grpc::ServerContext* context,
                          const kvs_cluster::GetUpdateRequest* request,
                          kvs_cluster::GetUpdateReply* reply) override {
    std::lock_guard<std::mutex> lock(db_mutex_);

    string_db_[request->key()] = request->val();

    return grpc::Status::OK;
  }

  grpc::Status diediedie(grpc::ServerContext* context,
                          const kvs_cluster::DieRequest* request,
                          kvs_cluster::DieReply* reply) override {

    kill(getpid(), SIGKILL);
    return grpc::Status::OK;
  }

private:
  // The actual database.
  // std::map<std::string, std::string> string_db_;

  // Mutex serializing access to the map.
  std::mutex db_mutex_;
};

// Communication with master.
class KVScluster_master
{
 public:
  KVScluster_master(std::shared_ptr<Channel> channel)
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

// Communication with neighbour.
class KVScluster_neighbour
{
 public:
  KVScluster_neighbour(std::shared_ptr<Channel> channel)
      : stub_(KVS_cluster_service::NewStub(channel)) {}

  // Assembles the client's payload, sends it and presents the response back
  // from the server.
  std::string selfAddr()
  {
    SendSelfAddrRequest request;

    SendSelfAddrReply reply;

    ClientContext context;

    gpr_timespec timeOut;

    timeOut.tv_sec = 15;
    timeOut.tv_nsec = 0;
    timeOut.clock_type = GPR_TIMESPAN;
    context.set_deadline(timeOut);

    Status status = stub_->selfAddr(&context, request, &reply);

    if (!status.ok() && status.error_code() == grpc::DEADLINE_EXCEEDED)
    {
        // call timeout handler
    }

    if (status.ok())
    {
      return reply.addr();
    }
    else
    {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

  std::string clusterMaster()
  {
    GetMasterInitRequest request;

    GetMasterInitReply reply;

    ClientContext context;

    gpr_timespec timeOut;

    timeOut.tv_sec = 15;
    timeOut.tv_nsec = 0;
    timeOut.clock_type = GPR_TIMESPAN;
    context.set_deadline(timeOut);

    Status status = stub_->clusterMaster(&context, request, &reply);

    if (!status.ok() && status.error_code() == grpc::DEADLINE_EXCEEDED)
    {
        // call timeout handler
    }

    if (status.ok())
    {
      return reply.ret();
    }
    else
    {
      std::cout << status.error_code() << ": " << status.error_message()
                << std::endl;
      return "RPC failed";
    }
  }

  std::string put(const std::string& row, const std::string& col, std::string& val)
  {
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

    gpr_timespec timeOut;

    timeOut.tv_sec = 15;
    timeOut.tv_nsec = 0;
    timeOut.clock_type = GPR_TIMESPAN;
    context.set_deadline(timeOut);

    // The actual RPC.
    Status status = stub_->put(&context, request, &reply);

    if (!status.ok() && status.error_code() == grpc::DEADLINE_EXCEEDED)
    {
        // call timeout handler
    }

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

    gpr_timespec timeOut;

    timeOut.tv_sec = 15;
    timeOut.tv_nsec = 0;
    timeOut.clock_type = GPR_TIMESPAN;
    context.set_deadline(timeOut);

    // The actual RPC.
    Status status = stub_->cput(&context, request, &reply);

    if (!status.ok() && status.error_code() == grpc::DEADLINE_EXCEEDED)
    {
        // call timeout handler
    }

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

    gpr_timespec timeOut;

    timeOut.tv_sec = 15;
    timeOut.tv_nsec = 0;
    timeOut.clock_type = GPR_TIMESPAN;
    context.set_deadline(timeOut);

    // The actual RPC.
    Status status = stub_->get(&context, request, &reply);

    if (!status.ok() && status.error_code() == grpc::DEADLINE_EXCEEDED)
    {
        // call timeout handler
    }

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

    gpr_timespec timeOut;

    timeOut.tv_sec = 15;
    timeOut.tv_nsec = 0;
    timeOut.clock_type = GPR_TIMESPAN;
    context.set_deadline(timeOut);

    // The actual RPC.
    Status status = stub_->del(&context, request, &reply);

    if (!status.ok() && status.error_code() == grpc::DEADLINE_EXCEEDED)
    {
        // call timeout handler
    }

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

  std::string get_update(const std::string& key, const std::string& val)
  {
    GetUpdateRequest request;
    request.set_key(key);
    request.set_val(val);

    GetUpdateReply reply;

    ClientContext context;

    gpr_timespec timeOut;

    timeOut.tv_sec = 15;
    timeOut.tv_nsec = 0;
    timeOut.clock_type = GPR_TIMESPAN;
    context.set_deadline(timeOut);

    Status status = stub_->get_update(&context, request, &reply);

    if (!status.ok() && status.error_code() == grpc::DEADLINE_EXCEEDED)
    {
        // call timeout handler
    }

    if (status.ok())
    {
      return "Good";
      //return reply.ret();
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

void RunServer(std::string addr)
{
  // Server listens on this address.
  // std::string server_address("0.0.0.0:60051");
  std::cout << "addr: " << addr << std::endl;
  std::string server_address = addr;
  KVS_clusterImpl service;

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

int smllst(int x, int y, int z)
{
    return std::min(std::min(x, y), z);
}

int lrgst(int x, int y, int z)
{
    return std::max(std::max(x, y), z);
}

int main(int argc, char** argv)
{
  // opt
  if (argc < 2)
  {
    fprintf(stderr, "*** Author: Mayank Darbari (mdarbari) \n");
    exit(1);
  }

  int c;
  int aflag;
  int mflag;
  int gflag;
  int n1flag;
  int n2flag;
  int fflag;
  char *aport;
  char *mport;
  char *gport;
  char *n1port;
  char *n2port;
  char *fport;

  static char usage[] = "usage: %s [-a <own_portno> -x <neigh1_portno> -y <neigh2_portno> -m <is_master> -g <clstr> -f <filename>] \n";

  opterr = 0;

  while((c = getopt (argc, argv, "a:x:y:m:g:f:")) != -1)
  {
  switch (c)
    {
      case 'a':
        aflag = 1;
        aport = optarg;
        break;
      case 'x':
        n1flag = 1;
        n1port = optarg;
        break;
      case 'y':
        n2flag = 1;
        n2port = optarg;
        break;
      case 'm':
        mflag = 1;
        mport = optarg;
        break;
      case 'g':
        gflag = 1;
        gport = optarg;
        break;
      case 'f':
        fflag = 1;
        fport = optarg;
        break;
      case '?':
        if(optopt == 'a')
          fprintf(stderr, "Option -%c requires an argument. \n", optopt);
        else if(isprint (optopt))
          fprintf(stderr, "Unknown option `-%c'. \n", optopt);
        else
          fprintf(stderr, "Unknown option character `\\x%x'. \n", optopt);
        exit(1);
      default:
        fprintf(stderr, "Unknown option `-%c'. \n", optopt);
        exit(1);
        break;
    }
  }

  if(!aflag)
  {
    fprintf(stderr, usage, argv[0]);
    fprintf(stderr, "Please specify own addr port. \n");
    exit(1);
  }

  if(!n1flag)
  {
    fprintf(stderr, usage, argv[0]);
    fprintf(stderr, "Please specify neighbour1 addr port. \n");
    exit(1);
  }

  if(!n2flag)
  {
    fprintf(stderr, usage, argv[0]);
    fprintf(stderr, "Please specify neighbour2 addr port. \n");
    exit(1);
  }

  if(!mflag)
  {
    fprintf(stderr, usage, argv[0]);
    fprintf(stderr, "Please specify is_master? \n");
    exit(1);
  }

  if(!gflag)
  {
    fprintf(stderr, usage, argv[0]);
    fprintf(stderr, "Please specify cluster? \n");
    exit(1);
  }

  if(!fflag)
  {
    fprintf(stderr, usage, argv[0]);
    fprintf(stderr, "Please specify filename. \n");
    exit(1);
  }

  //std::string fname = std::to_string(fport);
  std::cout << "fname:" << fport << std::endl;
  std::ifstream file(fport);

  std::string line;

  while(std::getline(file, line))
  {
      std::stringstream l(line);
      std::string value;
      int chk = 0;
      std::string temp1;
      std::string temp2;

      while(std::getline(l, value, ','))
      {
          if(chk == 0)
          {
            temp1 = value;
            chk++;
            continue;
          }
          else if(chk == 1)
          {
            temp2 = value;
            chk++;
            continue;
          }
          string_db_[(temp1 + "," + temp2)] = value;
          chk = 0;
          //std::cout << "Value(" << value << ")\n";
      }
      //std::cout << "Line Finished" << std::endl;
  }
  file.close();

  Booker bkr;

  std::string own_addr = "localhost:";
  own_addr = own_addr + (const char*)aport;
  std::cout << "Own addr: " << own_addr << std::endl;
  oa = own_addr;

  std::string neigh_addr1 = "localhost:";
  neigh_addr1 = neigh_addr1 + (const char*)n1port;
  std::cout << "Neigh1 addr: " << neigh_addr1 << std::endl;

  std::string neigh_addr2 = "localhost:";
  neigh_addr2 = neigh_addr2 + (const char*)n2port;
  std::cout << "Neigh2 addr: " << neigh_addr2 << std::endl;

  int is_mstr = atoi(mport);
  std::cout << "is_mstr?! " << is_mstr << std::endl;
  
  if(is_mstr)
  {
    bkr.set_ClusterMaster(own_addr);
  }
  else
  {
    bkr.set_ClusterMaster("nope");
  }

  im = is_mstr;

  std::string clstr_grp = gport;
  std::cout << "Cluster: " << clstr_grp << std::endl;

  int max_addr_used = lrgst(std::stoi((const char*)aport), std::stoi((const char*)n1port), std::stoi((const char*)n2port));
  std::cout << "max_addr_used: " << max_addr_used << std::endl;

  int max_obj_no = 2;

  //RunServer();
  std::thread thrd(&RunServer, own_addr);

  std::this_thread::sleep_for(std::chrono::seconds(3));

  if(is_mstr)
  {
    string_db_[("val1_" + own_addr)] = "Hello?,1";
    string_db_[("val2_" + own_addr)] = "Can,2";
    string_db_[("val3_" + own_addr)] = "You,3";
    string_db_[("val4_" + own_addr)] = "Hear,4";
    string_db_[("val5_" + own_addr)] = "Me,5";
  }

  std::cout << std::endl;
  std::cout << "TABLE: " << own_addr << std::endl;
  for(std::map<std::string,std::string>::iterator it=string_db_.begin(); it!=string_db_.end(); it++)
  {
    std::cout << it->first << " : " << it->second << std::endl;
  }
  std::cout << std::endl;

  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint (in this case,
  // localhost at port 50051). We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).
  // KVScluster_master obj(grpc::CreateChannel(
  //     "localhost:50051", grpc::InsecureChannelCredentials()));

  std::shared_ptr<grpc::Channel> mstr_chnnl = grpc::CreateChannel(
      "localhost:50051", grpc::InsecureChannelCredentials());

  KVScluster_master obj(mstr_chnnl);

  // KVScluster_neighbour objz1(grpc::CreateChannel(
  //   neigh_addr1, grpc::InsecureChannelCredentials()));

  std::shared_ptr<grpc::Channel> chnnl1 = grpc::CreateChannel(
      neigh_addr1, grpc::InsecureChannelCredentials());

  // KVScluster_neighbour objz1(chnnl1);

  KVScluster_neighbour *objz1 = new KVScluster_neighbour(chnnl1);

  // KVScluster_neighbour objz2(grpc::CreateChannel(
  //   neigh_addr2, grpc::InsecureChannelCredentials()));

  std::shared_ptr<grpc::Channel> chnnl2 = grpc::CreateChannel(
      neigh_addr2, grpc::InsecureChannelCredentials());

  // KVScluster_neighbour objz2(chnnl2);

  KVScluster_neighbour *objz2 = new KVScluster_neighbour(chnnl2);

  // KVScluster_neighbour objz3(grpc::CreateChannel(
  //   "0.0.0.0:60054", grpc::InsecureChannelCredentials()));

  // std::shared_ptr<grpc::Channel> chnnl3 = grpc::CreateChannel(
  //     "localhost:60059", grpc::InsecureChannelCredentials());

  // KVScluster_neighbour objz3(chnnl3);

  // KVScluster_neighbour *objz3 = new KVScluster_neighbour(chnnl3);

  if(!is_mstr)
  {
    std::this_thread::sleep_for(std::chrono::seconds(7));
  }

  // Master sends update on startup
  if(is_mstr)
  {
    for(std::map<std::string,std::string>::iterator it=string_db_.begin(); it!=string_db_.end(); it++)
    {
      objz1->get_update(it->first, it->second);
      objz2->get_update(it->first, it->second);
    }
    std::this_thread::sleep_for(std::chrono::seconds(3));
  }

  std::cout << std::endl;
  std::cout << "TABLE: " << own_addr << std::endl;
  for(std::map<std::string,std::string>::iterator it=string_db_.begin(); it!=string_db_.end(); it++)
  {
    std::cout << it->first << " : " << it->second << std::endl;
  }
  std::cout << std::endl;

  //List of objects
  // std::list<KVScluster_neighbour*> objzSet;
  // objzSet.insert(objzSet.end(), objz1);
  // objzSet.insert(objzSet.end(), objz2);
  // objzSet.insert(objzSet.end(), objz3);

  std::this_thread::sleep_for(std::chrono::seconds(3));

  //Comm with master
  std::string sendaddr_reply;
  std::string setleader_reply;

  sendaddr_reply = obj.send_addr();
  std::cout << "Master addr: " << sendaddr_reply << std::endl;

  bkr.set_masterAddr(sendaddr_reply);
  std::cout << "Master addr (bk): " << bkr.get_masterAddr() << std::endl;

  bkr.set_selfAddr(own_addr);
  std::cout << "Self addr (bk): " << bkr.get_selfAddr() << std::endl;

  bkr.set_N1Addr(neigh_addr1);
  std::cout << "N1 addr (bk): " << bkr.get_N1Addr() << std::endl;

  bkr.set_N2Addr(neigh_addr2);
  std::cout << "N2 addr (bk): " << bkr.get_N2Addr() << std::endl;

  if(is_mstr)
  {
    setleader_reply = obj.set_leader(clstr_grp, own_addr);
    std::cout << "Leader set: " << setleader_reply << std::endl;
  }

  int loop_no = 1;

  std::string sa1_reply;
  std::string cm1_reply;

  std::string sa2_reply;
  std::string cm2_reply;

  std::cout << "GRPC_CHANNEL_CONNECTING: " << GRPC_CHANNEL_CONNECTING << std::endl;
  std::cout << "GRPC_CHANNEL_READY: " << GRPC_CHANNEL_READY << std::endl;
  std::cout << "GRPC_CHANNEL_TRANSIENT_FAILURE: " << GRPC_CHANNEL_TRANSIENT_FAILURE << std::endl;
  std::cout << "GRPC_CHANNEL_IDLE: " << GRPC_CHANNEL_IDLE << std::endl;
  std::cout << "GRPC_CHANNEL_SHUTDOWN: " << GRPC_CHANNEL_SHUTDOWN << std::endl;

  // GRPC_CHANNEL_CONNECTING: 1
  // GRPC_CHANNEL_READY: 2
  // GRPC_CHANNEL_TRANSIENT_FAILURE: 3
  // GRPC_CHANNEL_IDLE: 0
  // GRPC_CHANNEL_SHUTDOWN: 4

  int addr_chk1;
  int addr_chk2;

  // WHILE
  while(1)
  {   

    auto chnnl1_state = chnnl1->GetState(true);
    std::cout << "Channel1 State: " << chnnl1_state << std::endl;

    if(sa1_reply == "RPC failed")
    {
      //chnnl1.shutdown();
      //bkr.set_N1Addr("localhost:99999");
      std::cout << "Here1" << std::endl;
      chnnl1 = NULL;
      std::string new_addr = "localhost:";
      //max_addr_used = max_addr_used + 1;
      new_addr = new_addr + std::to_string(max_addr_used+1);
      std::cout << "new_addr: " << new_addr << std::endl;
      chnnl1 = grpc::CreateChannel(new_addr, grpc::InsecureChannelCredentials());
      //objzSet.remove(objz1);
      delete objz1;
      objz1 = new KVScluster_neighbour(chnnl1);
      //objzSet.insert(objzSet.end(), objz1);
      //KVScluster_neighbour objz1(chnnl1);

      if(bkr.get_ClusterMaster() == bkr.get_selfAddr())
      {
        for(std::map<std::string,std::string>::iterator it=string_db_.begin(); it!=string_db_.end(); it++)
        {
          objz1->get_update(it->first, it->second);
        }
      }
      else
      {
        std::this_thread::sleep_for(std::chrono::seconds(5));
      }
    }

    auto chnnl2_state = chnnl2->GetState(true);
    std::cout << "Channel2 State: " << chnnl2_state << std::endl;

    if(sa2_reply == "RPC failed")
    {
      //chnnl2.shutdown();
      //bkr.set_N2Addr("localhost:99999");
      std::cout << "Here2" << std::endl;
      chnnl2 = NULL;
      std::string new_addr = "localhost:";
      //max_addr_used = max_addr_used + 1;
      new_addr = new_addr + std::to_string(max_addr_used+1);
      std::cout << "new_addr: " << new_addr << std::endl;
      chnnl2 = grpc::CreateChannel(new_addr, grpc::InsecureChannelCredentials());
      //objzSet.remove(objz2);
      delete objz2;
      objz2 = new KVScluster_neighbour(chnnl2);
      //objzSet.insert(objzSet.end(), objz2);
      //KVScluster_neighbour objz2(chnnl2);

      if(bkr.get_ClusterMaster() == bkr.get_selfAddr())
      {
        for(std::map<std::string,std::string>::iterator it=string_db_.begin(); it!=string_db_.end(); it++)
        {
          objz2->get_update(it->first, it->second);
        }
      }
      else
      {
        std::this_thread::sleep_for(std::chrono::seconds(5));
      }
    }

    // auto chnnl3_state = chnnl3->GetState(true);
    // std::cout << "Channel3 State: " << chnnl3_state << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(3));

    std::cout << "Loop_No: " << loop_no << std::endl;
    
    std::cout << std::endl;
    std::cout << "TABLE: " << own_addr << std::endl;
    for(std::map<std::string,std::string>::iterator it=string_db_.begin(); it!=string_db_.end(); it++)
    {
      std::cout << it->first << " : " << it->second << std::endl;
    }
    std::cout << std::endl;    

    //sa1_reply = objz1.selfAddr();
    sa1_reply = objz1->selfAddr();
    std::cout << "NeighbourAddr: " << sa1_reply << std::endl;
    if(sa1_reply != "RPC failed")
    {
      bkr.set_N1Addr(sa1_reply);
    }

    if(sa1_reply == "RPC failed")
    {
      addr_chk1 = 99999;
    }
    else
    {
      addr_chk1 = std::stoi((bkr.get_N1Addr()).substr(10, (bkr.get_N1Addr()).length()-10));
    }  

    //cm1_reply = objz1.clusterMaster();
    cm1_reply = objz1->clusterMaster();
    std::cout << "ClusterMaster: " << cm1_reply << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(3));

    // sa2_reply = objz2.selfAddr();
    sa2_reply = objz2->selfAddr();
    std::cout << "NeighbourAddr: " << sa2_reply << std::endl;
    if(sa2_reply != "RPC failed")
    {
      bkr.set_N2Addr(sa2_reply);
    }

    if(sa2_reply == "RPC failed")
    {
      addr_chk2 = 99999;
    }
    else
    {
      addr_chk2 = std::stoi((bkr.get_N2Addr()).substr(10, (bkr.get_N2Addr()).length()-10));
    }  

    // cm2_reply = objz2.clusterMaster();
    cm2_reply = objz2->clusterMaster();
    std::cout << "ClusterMaster: " << cm2_reply << std::endl;

    if(cm1_reply == "yes")
    {
      bkr.set_ClusterMaster(sa1_reply);
      std::cout << "Cluster Master: " << bkr.get_ClusterMaster() << std::endl;
    }
    else if(cm2_reply == "yes")
    {
      bkr.set_ClusterMaster(sa2_reply);
      std::cout << "Cluster Master: " << bkr.get_ClusterMaster() << std::endl;
    }
    else if(bkr.get_ClusterMaster() == bkr.get_selfAddr())
    {
      bkr.set_ClusterMaster(own_addr);
      std::cout << "Cluster Master: " << bkr.get_ClusterMaster() << std::endl;
    }
    else  //leader election
    {
      int ldr = smllst(std::stoi((bkr.get_selfAddr()).substr(10, (bkr.get_selfAddr()).length()-10)),
        addr_chk1, addr_chk2);
      bkr.set_ClusterMaster(("localhost:" + std::to_string(ldr)));
      std::cout << "Leader Election Cluster Master: " << bkr.get_ClusterMaster() << std::endl;
      obj.set_leader(gport, bkr.get_ClusterMaster());
      if(bkr.get_ClusterMaster() == bkr.get_selfAddr())
      {
        is_mstr = 1;
        im = 1;
      }
    }

    if(is_mstr)
    {
      std::cout << "Inside print loop" << std::endl;
      std::ofstream outfile;
      outfile.open(fport, std::ofstream::out);
      for(std::map<std::string,std::string>::iterator it=string_db_.begin(); it!=string_db_.end(); it++)
      {
        std::cout << "Inside loop for" << std::endl;
        //std::this_thread::sleep_for(std::chrono::seconds(1));
        outfile << trim_copy(it->first);
        outfile << ",";
        outfile << trim_copy(it->second);
        outfile << "\n";
        //std::this_thread::sleep_for(std::chrono::seconds(1));
        outfile.flush();
        objz1->get_update(it->first, it->second);
        objz2->get_update(it->first, it->second);
      }
      std::cout << "Flushing" << std::endl;
      outfile.close();
      std::this_thread::sleep_for(std::chrono::seconds(3));
    }
    else
    {
      std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    max_addr_used = lrgst(std::stoi((bkr.get_selfAddr()).substr(10, (bkr.get_selfAddr()).length()-10)),
      std::stoi((bkr.get_N1Addr()).substr(10, (bkr.get_N1Addr()).length()-10)),
      std::stoi((bkr.get_N2Addr()).substr(10, (bkr.get_N2Addr()).length()-10)));

    std::cout << "max_addr_used: " << max_addr_used << std::endl;

    loop_no++;

  }

  thrd.join();

  return 0;
}