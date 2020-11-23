#!/usr/bin/bash

rm *.dat
URL=${1:-http://ipv4.download.thinkbroadband.com/5MB.zip}
HTTP_CHUNK=${2:-100000}
IO_CHUNK=${3:-1000000}

./downloader.py "$URL" $HTTP_CHUNK $IO_CHUNK

mkdir -p out
cat *.dat > out/testfile

wget -O out/origfile "$URL"

md5sum out/testfile out/origfile

diff -s  out/testfile out/origfile 