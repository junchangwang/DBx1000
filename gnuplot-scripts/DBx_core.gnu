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

###################################### core_f
reset
set size 0.99,1.0
#set term pdf font ",10" size 5.2, 2.4
set ylabel "Latency (ms)" offset 1.5,0,0
set xlabel "Number of cores"
set xtics ("1" 1, "2" 2 1, "4" 4, "8" 8, "16" 16, "24" 24, "32" 32)
#set yrange [0:40]
set xrange [0:35]
set terminal jpeg font ',15'
set output "../graphs_DBx/core/core_f.jpeg"
set key top left

plot "../dat_DBx/core_f.dat" every 4::0::24 title "Scan" lc rgb "purple" ps 1 pt 9 with linespoints,\
      "../dat_DBx/core_f.dat" every 4::1::25 title "Hash" lc rgb "blue" ps 1 pt 8 with linespoints,\
      "../dat_DBx/core_f.dat" every 4::2::26 title "BTree" lc rgb "green" ps 1 pt 7 with linespoints,\
      "../dat_DBx/core_f.dat" every 4::3::27 title "CUBIT" lc rgb "red" ps 1 pt 6 with linespoints

set terminal eps font ',15'
set output "../graphs_DBx/core/core_f.eps"
replot

############################## histograms###############################
reset
set key invert top left

set terminal jpeg font ',15'
set output "../graphs_DBx/core/histograms.jpeg"

set origin 0,0.05
set size 1,0.95

#set yrange [0:3e3]
set autoscale y

#set title "Breakdown of Updates"
set style data histograms
set style histogram rowstacked
set boxwidth 0.75 relative
set style fill solid 1.0 border -1
set xtics nomirror rotate by -45 scale 0

set ylabel "Latency (s)" offset 1,0,0

#unset xtics
set label "1" font ",13" at screen 0.25, screen 0.04
set label "2" font ",13" at screen 0.35, screen 0.04
set label "4" font ",13" at screen 0.45, screen 0.04
set label "8" font ",13" at screen 0.55, screen 0.04
set label "16" font ",13" at screen 0.65, screen 0.04
set label "24" font ",13" at screen 0.75, screen 0.04
set label "32" font ",13" at screen 0.85, screen 0.04

set xtics font ",8"

plot "../dat_DBx/index_time.dat" using ($3):xtic(4) title "Row" with histograms fill pattern 4 border rgb "black" lc rgb "black", \
     "../dat_DBx/index_time.dat" using ($2):xtic(4) title "Index" with histograms fs solid border rgb "black" lc rgb "black"

set terminal eps font ',15'
set output "../graphs_DBx/core/histograms.eps"
replot
