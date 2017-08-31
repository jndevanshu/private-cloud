pkill loadbalancer
pkill httpserver
pkill kvs_master
pkill kvs_cluster

rm *.err.log
rm *.out.log

echo "" > f1.txt
echo "" > f2.txt
echo "" > f3.txt

./loadbalancer -v -h 8000 -l 5000 1>loadbalancer.out.log 2>loadbalancer.err.log &

./httpserver -v -h 8001 -l 5000 1>http1.out.log 2>http1.err.log &
./httpserver -v -h 8002 -l 5000 1>http2.out.log 2>http2.err.log &
./httpserver -v -h 8003 -l 5000 1>http3.out.log 2>http3.err.log &

./kvs_master 1>master.out.log 2>master.err.log &

./kvs_cluster -a 60051 -x 60052 -y 60053 -m 1 -g a -f f1.txt 1>cluster_a1.out.log 2>cluster_a1.err.log &
./kvs_cluster -a 60052 -x 60053 -y 60051 -m 0 -g a -f f1.txt 1>cluster_a2.out.log 2>cluster_a2.err.log &
./kvs_cluster -a 60053 -x 60052 -y 60051 -m 0 -g a -f f1.txt 1>cluster_a3.out.log 2>cluster_a3.err.log &

./kvs_cluster -a 61051 -x 61052 -y 61053 -m 1 -g k -f f2.txt 1>cluster_k1.out.log 2>cluster_k1.err.log &
./kvs_cluster -a 61052 -x 61053 -y 61051 -m 0 -g k -f f2.txt 1>cluster_k2.out.log 2>cluster_k2.err.log &
./kvs_cluster -a 61053 -x 61052 -y 61051 -m 0 -g k -f f2.txt 1>cluster_k3.out.log 2>cluster_k3.err.log &

./kvs_cluster -a 62051 -x 62052 -y 62053 -m 1 -g u -f f3.txt 1>cluster_u1.out.log 2>cluster_u1.err.log &
./kvs_cluster -a 62052 -x 62053 -y 62051 -m 0 -g u -f f3.txt 1>cluster_u2.out.log 2>cluster_u2.err.log &
./kvs_cluster -a 62053 -x 62052 -y 62051 -m 0 -g u -f f3.txt 1>cluster_u3.out.log 2>cluster_u3.err.log &
