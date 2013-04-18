#!/bin/bash
 
isEven(){
    rem=$(($1 % 2))
    return $rem
}

while [ $# -gt 0 ] ; do
    nodeArg=$1
  ip=$(echo $nodeArg | awk -F":" '{print $1}') 
  port=$(echo $nodeArg | awk -F":" '{print $2}') 


  #ip=${var[0]}
  #port=${var[1]}
  ip_arr=$(echo $ip | awk -F"." '{print $4}') 
  shift 
  if  isEven ${ip_arr} -eq 0 && isEven ${port} -eq 0; then
      echo -n "/rack1"
  else
      echo -n "/rack2"
  fi
done 
