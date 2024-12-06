#!/bin/bash
cd $(dirname $0)

data_dir="packages"
declare -A data
data["spark"]="${SOURCE_SPARK}"

function install_software() {
  software=$1
  s=$(rpm -qa | grep ${software})
  result=$(echo ${s} | grep ${software})
  if [[ "$result" != "" ]]; then
    echo "${software} is already installed"
  else
    yum -y install ${software}
  fi
}

install_software wget

if [ ! -d "./$data_dir" ]; then
  mkdir $data_dir
fi

for module in "${!data[@]}"; do
  if [ ! -d "./$data_dir/$module" ]; then
    mkdir -p $data_dir/$module
  fi
  for item in ${data[$module]}; do
    if [ ! -f "./$data_dir/$module/$item" ]; then
      echo "./$data_dir/$module/$item 不在本地，开始从远端下载..."
      wget -O ./$data_dir/$module/$item ${DOWNLOAD_BASE}/$module/$item
    fi
  done
done
