#!/bin/bash

while true; do
echo "$(date) =====================================================================================" >> versioning_benchmark-controller.log
echo "$(date) =====================================================================================" >> versioning_data-generators.log
echo "$(date) =====================================================================================" >> versioning_virtuoso-system.log
echo "$(date) =====================================================================================" >> versioning_task-generators.log
echo "$(date) =====================================================================================" >> versioning_evaluation-module.log
echo "$(date) =====================================================================================" >> versioning_virtuoso-gold-standard.log


data_gens=( $(docker ps -aqf "ancestor=git.project-hobbit.eu:4567/papv/versioningdatagenerator") )
task_gens=( $(docker ps -aqf "ancestor=git.project-hobbit.eu:4567/papv/versioningtaskgenerator") )
bench=( $(docker ps -aqf "ancestor=git.project-hobbit.eu:4567/papv/versioningbenchmarkcontroller") )
system=( $(docker ps -aqf "ancestor=git.project-hobbit.eu:4567/papv/versioningsystem") )
eval_mod=( $(docker ps -aqf "ancestor=git.project-hobbit.eu:4567/papv/versioningevaluationmodule") )
virt_gs=( $(docker ps -aqf "ancestor=tenforce") )

for data_gen in "${data_gens[@]}"; do
   docker logs $data_gen  >> versioning_data-generators.log
   echo "-------------------------------------------------------------------------------------" >> versioning_data-generators.log
   echo Data generators logs saved.
done

for task_gen in "${task_gens[@]}"; do
   docker logs $task_gen  >> versioning_task-generators.log
   echo "-------------------------------------------------------------------------------------" >> versioning_task-generators.log
   echo Task generators logs saved.
done

for ben in "${bench[@]}"; do
   docker logs $ben  >> versioning_benchmark-controller.log
   echo "-------------------------------------------------------------------------------------" >> versioning_benchmark-controller.log
   echo Benchmark controller logs saved.
done

for sys in "${system[@]}"; do
   docker logs $sys  >> versioning_virtuoso-system.log
   echo "-------------------------------------------------------------------------------------" >> versioning_virtuoso-system.log
   echo Test system logs saved.
done

for ev_mod in "${eval_mod[@]}"; do
   docker logs $ev_mod  >> versioning_evaluation-module.log
   echo "-------------------------------------------------------------------------------------" >> versioning_evaluation-module.log
   echo Evaluation module logs saved.
done

for gs in "${virt_gs[@]}"; do
   docker logs $gs  >> versioning_virtuoso-gold-standard.log
   echo "-------------------------------------------------------------------------------------" >> versioning_virtuoso-gold-standard.log
   echo Gold standard logs saved.
done

sleep 1
done
