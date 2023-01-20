reset
set size 0.99,1.0
#set term pdf font ",10" size 5.2, 2.4
set ylabel "Throughput (transactions/s)" offset 1.5,0,0 font ',20'
set xlabel "Number of cores" font ',20'
set xtics ("1" 1, "2" 2, "4" 4, "8" 8, "16" 16, "24" 24, "32" 32)
set yrange [0:]
set xrange [0:35]
set terminal png font ',15'
set output "../graphs_DBx/core/core.png"
set key reverse top left Left

plot "../dat_DBx/core.dat" every 4::3::27 title "CUBIT" lc rgb "blue" lw 7 ps 1.5 pt 6 dt 4 with linespoints,\
      "../dat_DBx/core.dat" every 4::2::26 title "B^+-Tree" lc rgb "orange" lw 7 ps 1.5 pt 8 dt 8 with linespoints,\
      "../dat_DBx/core.dat" every 4::1::25 title "Hash" lc rgb "green" lw 7 ps 1.5 pt 10 dt 3 with linespoints,\
      "../dat_DBx/core.dat" every 4::0::24 title "Scan" lc rgb "black" lw 7 ps 1.5 pt 12 dt 7 with linespoints

set terminal eps font ',16.5' enhanced
set output "../graphs_DBx/core/core.eps"
replot

###################################### core_f
# reset
# set size 0.99,1.0
# #set term pdf font ",10" size 5.2, 2.4
# set ylabel "Latency (ms)" offset 1.5,0,0
# set xlabel "Number of cores"
# set xtics ("1" 1, "2" 2 1, "4" 4, "8" 8, "16" 16, "24" 24, "32" 32)
# set yrange [0:]
# set xrange [0:35]
# set terminal png font ',15'
# set output "../graphs_DBx/core/core_f.png"
# set key reverse top left Left

# plot "../dat_DBx/core_f.dat" every 4::0::24 title "Scan" lc rgb "black" lw 7 ps 1.5 pt 12 dt 7 with linespoints,\
#       "../dat_DBx/core_f.dat" every 4::1::25 title "Hash" lc rgb "green" lw 7 ps 1.5 pt 10 dt 3 with linespoints,\
#       "../dat_DBx/core_f.dat" every 4::2::26 title "BTree" lc rgb "orange" lw 7 ps 1.5 pt 8 dt 8 with linespoints,\
#       "../dat_DBx/core_f.dat" every 4::3::27 title "CUBIT" lc rgb "blue" lw 7 ps 1.5 pt 6 dt 4 with linespoints

# set terminal eps font ',15' size 4, 5
# set output "../graphs_DBx/core/core_f.eps"
# replot

############################## histograms index and tuples ###############################
reset
set key reverse invert top right Left

set terminal png font ',15'
set output "../graphs_DBx/core/histograms.png"

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

set ylabel "Latency (ms)" offset 1,0,0 font ',20'

#unset xtics
set label "1" font ",18" at screen 0.19, screen 0.04
set label "2" font ",18" at screen 0.3, screen 0.04
set label "4" font ",18" at screen 0.415, screen 0.04
set label "8" font ",18" at screen 0.535, screen 0.04
set label "16" font ",18" at screen 0.64, screen 0.04
set label "24" font ",18" at screen 0.75, screen 0.04
set label "32(cores)" font ",18" at screen 0.83, screen 0.04

set xtics font ",8"

plot "../dat_DBx/index_time.dat" using ($3/1e3):xtic(4) title "Fetching tuples" with histograms fill pattern 7 border rgb "black" lc rgb "black", \
      "../dat_DBx/index_time.dat" using ($2/1e3):xtic(4) title "Indexing" with histograms fs solid border rgb "black" lc rgb "black"
     

set terminal eps font ',15' enhanced
set output "../graphs_DBx/core/histograms.eps"
replot

############################## histograms run time ###############################
reset
set key reverse invert top right Left

set terminal png font ',15'
set output "../graphs_DBx/core/histograms_run_time.png"

set origin 0,0.05
set size 1,0.95

#set yrange [0:3e3]
# set autoscale y
set boxwidth 0.8
set style fill solid
# set boxwidth 0.75 relative
# set style fill solid 1.0 border -1
# set xtics nomirror rotate by -45 scale 0
set ylabel "Latency (ms)" offset 1,0,0 font ',20'
set xrange [-0.5:28]
unset xtics
set label "1" font ",18" at screen 0.19, screen 0.04
set label "2" font ",18" at screen 0.3, screen 0.04
set label "4" font ",18" at screen 0.415, screen 0.04
set label "8" font ",18" at screen 0.535, screen 0.04
set label "16" font ",18" at screen 0.64, screen 0.04
set label "24" font ",18" at screen 0.75, screen 0.04
set label "32(cores)" font ",18" at screen 0.83, screen 0.04

# set xticsfont ",8"

# plot  "../dat_DBx/run_time.dat" every 3   using 1:($2/1e3) title "Hash" with histograms fs solid border rgb "black" lc rgb "black"

plot "../dat_DBx/run_time.dat" every 3    using 1:($2/1e3) title "Hash" with boxes fs solid 1.0 border rgb "black" lc rgb "white", \
     "../dat_DBx/run_time.dat" every 3::1 using 1:($2/1e3) title "B^+-Tree" with boxes fs solid 1.0 border rgb "black" lc rgb "grey", \
     "../dat_DBx/run_time.dat" every 3::2 using 1:($2/1e3) title "CUBIT" with boxes fs solid 1.0 border rgb "black" lc rgb "black"     

set terminal eps font ',17' enhanced
set output "../graphs_DBx/core/histograms_run_time.eps"
replot