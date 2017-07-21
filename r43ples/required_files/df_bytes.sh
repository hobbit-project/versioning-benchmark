#!/bin/bash

echo $(df -B1 / | awk '{print $3}' | tail -n 1)
