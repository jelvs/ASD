#!/bin/bash

port=2553
for i in $(seq 1 $1)
do
    gnome-terminal -x bash -c "echo ola"
    ((port++))
done

