#!/bin/bash

# go build main.go hash-join.go file-utils.go  

go test -bench=Main -cpuprofile=cpu.out -benchtime=1x
