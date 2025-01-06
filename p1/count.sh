#!/bin/bash

# Download the ZIP file
wget https://pages.cs.wisc.edu/~harter/cs544/data/hdma-wi-2021.zip

# Unzip the file
unzip hdma-wi-2021.zip

# Count lines containing "Multifamily" and display the result
grep "Multifamily" hdma-wi-2021.csv | wc -l
