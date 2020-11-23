#!/usr/bin/bash

rm *.dat

./downloader.py "http://ipv4.download.thinkbroadband.com/5MB.zip" ${1:-100000} ${2:-1000000}

mkdir -p xxout
cat *.dat > out/5MB.zip

wget  -O 5MB.orig http://ipv4.download.thinkbroadband.com/5MB.zip

md5sum out/5MB.zip out/5MB.orig

xxd out/5MB.zip > out/5MB.zip.text
xxd out/5MB.orig > out/5MB.orig.text
