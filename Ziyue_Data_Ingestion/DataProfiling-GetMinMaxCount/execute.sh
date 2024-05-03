#!/bin/bash

# Define index that need to get min/max/count
values=(2 4 6)
names=("new_persons_vaccinated" "new_persons_fully_vaccinated" "new_vaccine_doses_administered")

# Iterate over each index in the array
for((i=0; i < 3; i++)); do
    echo "hadoop jar DataIngestion.jar DataIngestionMinMax group_project/vaccinations.csv group_project/output_${names[i]} 1 ${values[i]} ,"
    hadoop jar DataIngestion.jar DataIngestionMinMax group_project/vaccinations.csv group_project/output_${names[i]} 1 ${values[i]} ,
done
