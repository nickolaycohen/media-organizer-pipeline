#!/bin/bash
echo "parameter #1: $1"

function traverse_directory {
  local directory="$1"

  echo "Running remove_mov.sh in $directory"
  (cp /Users/nickolaycohen/dev/media-organizer-pipeline/scripts/traverse_remove_mov/remove_mov.sh "$directory" && cd "$directory" && ./remove_mov.sh && rm remove_mov.sh)

  for item in "$directory"/*; do
    if [ -d "$item" ]; then
      echo "Traversing subfolder: $item"
      traverse_directory "$item"
    fi
  done
}

# Start traversing from the current directory
if [ -z "$1" ] 
  then 
    echo "no parameter - will traverse current folder ... "
    traverse_directory "."
  else
    echo "starting traverse from: $1"
    traverse_directory "$1"
fi


