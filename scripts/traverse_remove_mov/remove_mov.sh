#!/bin/bash

for mov_file in *.mov; do
    base_name="${mov_file%.*}"
    if [ -f "$base_name.HEIC" ]; then
        echo "Removing $mov_file because $base_name.HEIC exists"
        rm "$mov_file"
    fi
done
