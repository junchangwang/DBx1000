reset
set size 0.99,1.0
#set term pdf font ",10" size 5.2, 2.4
set ylabel "Throughput (transactions/s)" offset 1.5,0,0
set xlabel "Number of cores"
set xtics ("1" 1, "2" 2 1, "4" 4, "8" 8, "16" 16, "24" 24, "32" 32)
#set yrange [0:40]
set xrange [0:35]
set terminal jpeg font ',15'
set output "../graphs_DBx/core/core.jpeg"
set key top left

plot "../dat_DBx/core.dat" every 4::0::24 title "Scan" lc rgb "purple" ps 1 pt 9 with linespoints,\
      "../dat_DBx/core.dat" every 4::1::25 title "Hash" lc rgb "blue" ps 1 pt 8 with linespoints,\
      "../dat_DBx/core.dat" every 4::2::26 title "BTree" lc rgb "green" ps 1 pt 7 with linespoints,\
      "../dat_DBx/core.dat" every 4::3::27 title "CUBIT" lc rgb "red" ps 1 pt 6 with linespoints

set terminal eps font ',15'
set output "../graphs_DBx/core/core.eps"
replot