#!/bin/bash

cat "$1/config"* "$1/trace"* | sha256sum
cat "$2/config"* "$2/trace"* | sha256sum
