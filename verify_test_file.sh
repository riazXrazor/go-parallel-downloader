#!/usr/bin/env bash


if [ "$1" -eq 1 ]; then
   md5sum ./Big_Buck_Bunny_360_10s_1MB.mp4 ./verify_file/Big_Buck_Bunny_360_10s_1MB.mp4
else
    md5sum ./Big_Buck_Bunny_1080_10s_5MB.mp4 ./verify_file/Big_Buck_Bunny_1080_10s_5MB.mp4
fi

