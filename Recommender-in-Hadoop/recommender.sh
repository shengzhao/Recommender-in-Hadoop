#########################################################################
# File Name: recommender.sh
# Author: shengzhao
# mail: zhaosheng0509@gmail.com
# Created Time: Mon 14 Jan 2013 02:13:25 PM CST
#########################################################################
#!/bin/bash

if [ $# -lt 1 ] ; then
cat << HELP
	Please input the input dir
HELP
exit 0
fi

# define the const variable
MahoutHomeJar_Dir=/home/hadoop/workspace/mahout-distribution-0.5/core/target/mahout-core-0.5-job.jar
MahoutJobName=org.apache.mahout.cf.taste.hadoop.item.RecommenderJob

# cd the work dir
cd ${HADOOP_HOME}
bin/start-all.sh

# upload the data
da=$(date +'%Y%m%d%H%M%S')
InputDir=input${da}
TempDir=temp
if [ -f "${InputDir}" ]; then
	bin/hadoop dfs -rmr /user/hadoop/${InputDir}
fi
bin/hadoop dfs -rmr ${TempDir}
bin/hadoop dfs -put $1 ${InputDir}

# start mahout hadoop Job
OutputDir=output${da}
bin/hadoop jar ${MahoutHomeJar_Dir} ${MahoutJobName} -Dmapred.input.dir=${InputDir} -Dmapred.output.dir=${OutputDir}
