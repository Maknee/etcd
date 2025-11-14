#!/bin/bash

machines=$(cat external_ips) 
machines=$machines | tr '\n' ' '
cssh -l ramn $machines