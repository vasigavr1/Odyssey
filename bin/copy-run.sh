#!/usr/bin/env bash

source ./cluster.sh

IS_REMOTE_BQR="1"
BQR_READ_BUF_LEN="0"
WRITE_RATIO="-1"
EXEC="zookeeper"

# Each letter is an option argument, if it's followed by a collum
# it requires an argument. The first colum indicates the '\?'
# help/error command when no arguments are given
while getopts ":B:R:w:x:h" opt; do
  case $opt in
     x)
       EXEC=$OPTARG # given number is divided by 10 to give write rate % (i.e., 55 means 5.5 % writes)
       ;;
     w)
       WRITE_RATIO=$OPTARG # given number is divided by 10 to give write rate % (i.e., 55 means 5.5 % writes)
       ;;
     B)
       BQR_READ_BUF_LEN=$OPTARG # given number is divided by 10 to give write rate % (i.e., 55 means 5.5 % writes)
       ;;
     R)
       IS_REMOTE_BQR=$OPTARG # given number is divided by 10 to give write rate % (i.e., 55 means 5.5 % writes)
       ;;
     h)
      echo "Usage: -w <write ratio> -x <executable> -B <bqr_len> -R <is_remote_bqr> (x1000 --> 10 for 1%)"
      exit 1
      ;;
    \?)
      echo "Invalid option: -$OPTARG use -h to get info for arguments" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

#/home/s1687259/odyssey/bin/copy-kite-executables.sh "$1"
#EXECUTABLE="$1"
MAKE_FOLDER="${OD_HOME}/build"
SCRIPT_FOLDER="${OD_HOME}/bin"
DEST_FOLDER=$MAKE_FOLDER
SCRIPT="run-exe.sh"

${SCRIPT_FOLDER}/copy-executables.sh ${EXEC} $SCRIPT \
    $MAKE_FOLDER $SCRIPT_FOLDER $DEST_FOLDER

#cd  /home/s1687259/odyssey/build
#./run-exe.sh "$1"
cd $MAKE_FOLDER
sleep 2
$SCRIPT_FOLDER/$SCRIPT  -x "${EXEC}" -w "${WRITE_RATIO}" -B "${BQR_READ_BUF_LEN}" -R "${IS_REMOTE_BQR}"
