for wl in workload_b workload_c workload_d workload_f workload_a workload_read workload_insert workload_update workload_e;
do
	for hf in 1 2;
	do
		for hi in 0 1 2;
		do
			./bin/experiment_benchmark/ycsb_benchmark $wl $hf $hi 32 _new;
		done;
	done;
done
