#!/bin/bash
set -eu

table="$1"
source _dbgen.sh

dbgen "$table" 
