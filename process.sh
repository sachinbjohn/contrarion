FL="Expt,NumDCs,Key/Serv,#Serv,ValSize,Key/Read,WriteFrac,Zipf,NumClients,TotalThreads,LocalThreads,Client,NumOps,NumKeys,NumColumns,NumBytes,NumReads,NumWrites,Duration,Throughput,Ravg,R50,R90,R99,Wavg,W50,W90,W99,#Tx2R,#K2R,#aggR,#aggW,Lsum,Lavg,P_R,AVG_RD,AVG_W,AVG_OP,Xput,Real Xput"
echo $FL > processed/$1
cat $1 | awk -F"," {'pr=$17/($17+$18); avg_rd=pr*$21;avg_w=(1-pr)*$25;avg_op=(avg_rd+avg_w)/1000000;Xput=$11/avg_op;print $0","pr","avg_rd","avg_w","avg_op","Xput","$20'} >> processed/$1
# echo -e "$FL\n$(cat $1)" > $1