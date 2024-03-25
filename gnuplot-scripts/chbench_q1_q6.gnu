set terminal eps size 6, 2.5 font 'Linux Libertine O,12'  enhanced # 设置输出文件格式和字体

set output 'clustered_barchart.eps' # 设置输出文件名

set title "chbench query lantency" # 设置标题

# set xlabel "Query kind" offset 0,1.2 font ',25' # 设置x轴标签

set ylabel "Latency(ms)" offset 1.2,-0.3 font ',12' # 设置y轴标签

set yrange [0:3500] # 设置y轴范围

#set key font ',23' reverse vertical Left at graph 0.9, 0.92

set style data histogram # 设置数据样式为直方图

set style histogram clustered # 设置聚簇柱状图的样式

set style fill pattern # 设置填充样式为斜线

set xtics scale 0 #设置坐标轴刻度为空

set boxwidth 0.8 relative # 设置柱状图间距

set offsets -0.5, -0.5, 0, 0

# 设置图片边距
set lmargin 8.6
set rmargin 0.2
set tmargin 0.2
set bmargin 1.8



plot '../dat/chbench_q1.dat' using 2:xtic(1) lc rgb "black" title "SCAN" with histogram fill pattern 4, '' using 3 lc rgb "dark-orange" title "BTREE" with histogram fill pattern 4\
                        , '' using 4 lc rgb "brown" title "BWTREE" with histogram fill pattern 4, '' using 5 lc rgb "dark-violet" title "ART" with histogram fill pattern 4\
                        , '' using 6 lc rgb "blue" title "CUBIT" with histogram fill pattern 4, '' using 7 lc rgb "light-blue" title "CUBIT-PARA" with histogram fill pattern 4

