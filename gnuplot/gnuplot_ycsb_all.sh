for wl in workload_a workload_b workload_c workload_d workload_f workload_insert workload_update workload_read;
do
	for hf in tabulation murmur;
	do
		gnuplot -e "workload='$wl'" -e "hash_func='$hf'" gnuplot/gnuplot_ycsb_core_workload;
	done;
done