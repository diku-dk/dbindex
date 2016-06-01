for wl in workload_b workload_c workload_d workload_f workload_a;
do
	for hf in 2 1;
	do
		for hi in 0 1;
		do
			./bin/test/extendible_hash_table_ycsb $wl $hf $hi;
		done;
	done;
done
