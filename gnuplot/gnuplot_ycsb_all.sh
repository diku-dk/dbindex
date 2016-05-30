for wl in workload_a workload_b workload_c workload_d workload_f;
do
	for hf in tabulation murmur;
	do
		for hi in array extendible;
		do
			gnuplot -e "workload='$wl'" -e "hash_func='$hf'" -e "hash_index='$hi'" gnuplot_ycsb_core_workload;
		done;
	done;
done