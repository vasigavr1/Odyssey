INFORMATICS="informatics"
CLOUDLAB="cloudlab"
SETUP=$CLOUDLAB


if [ $SETUP == $INFORMATICS ]; then
  echo "informatics"
  OD_HOME="/home/s1687259/odyssey"
  HOSTS=(houston sanantonio )
  allIPs=( 129.215.165.8 #houston
        129.215.165.7 #sanantonio
        192.168.8.4 #houston
        192.168.8.6 #austin
        192.168.8.5 #sanantonio
        192.168.8.3 #indianapolis
        192.168.8.2 #philly
        192.168.5.11
        192.168.5.13
        )
  #localIP=$(ip addr | grep 'infiniband' -A2 | sed -n 2p | awk '{print $2}' | cut -f1  -d'/')
  localIP=$(ip addr | grep 'ether' -A2 | sed -n 2p | awk '{print $2}' | cut -f1  -d'/')
  NET_DEVICE_NAME="mlx5_1"
  IS_ROCE=1
else
  echo "cloudlab"
  OD_HOME="/users/vasigavr/odyssey"

  HOSTS=(
   10.0.3.1
   10.0.3.2
   10.0.3.3
   10.0.3.4
   10.0.3.5
  )

  allIPs=(
	10.0.3.1
	10.0.3.2
	10.0.3.3
	10.0.3.4
	10.0.3.5
	10.0.3.6
	10.0.3.7
	)

  localIP=$(ip addr | grep 'state UP' -A2 | grep 'inet 10.0.3'| awk '{print $2}' | cut -f1  -d'/')
  NET_DEVICE_NAME="mlx4_0"
  IS_ROCE=0
fi

##########################################
### NO NEED TO CHANGE BELOW THIS POINT ###
##########################################
REMOTE_IPS=${HOSTS[@]/$localIP}
REMOTE_HOSTS=${HOSTS[@]/$localIP}






### TO BE FILLED: Modify to get the local IP of the node running the script (must be one of the cluster nodes)
#cloudlab

#Informatics
#localIP=$(ip addr | grep 'infiniband' -A2 | sed -n 2p | awk '{print $2}' | cut -f1  -d'/')
#localIP=$(ip addr | grep 'ether' -A2 | sed -n 2p | awk '{print $2}' | cut -f1  -d'/')

### Fill the RDMA device name (the "hca_id" of the device when executing ibv_devinfo)
#NET_DEVICE_NAME="mlx4_0"  # cloudlab
#NET_DEVICE_NAME="mlx5_1" # informatics




