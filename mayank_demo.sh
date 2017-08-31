./kvs_cluster -a 60054 -x 60053 -y 60055 -m 0 -g a -f f1.txt 1>cluster_a4.out.log 2>cluster_a4.err.log &

./kvs_cluster -a 60055 -x 60053 -y 60054 -m 0 -g a -f f1.txt 1>cluster_a5.out.log 2>cluster_a5.err.log &

./kvs_cluster -a 60056 -x 60057 -y 60058 -m 0 -g a -f f1.txt 1>cluster_a6.out.log 2>cluster_a6.err.log &

./kvs_cluster -a 60057 -x 60056 -y 60057 -m 0 -g a -f f1.txt 1>cluster_a7.out.log 2>cluster_a7.err.log &

./kvs_cluster -a 60058 -x 60057 -y 60056 -m 0 -g a -f f1.txt 1>cluster_a8.out.log 2>cluster_a8.err.log &
