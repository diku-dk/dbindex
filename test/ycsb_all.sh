for wl in workload_a workload_b workload_c workload_d workload_f;
do
	for hf in 1 2;
	do
		for hi in 0 1;
		do
			./bin/test/extendible_hash_table_ycsb $wl $hf $hi;
		done;
	done;
done
