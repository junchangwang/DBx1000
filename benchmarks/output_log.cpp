#include<iostream>
#include<string>
#include "output_log.h"
#include "chbench.h"
using namespace std;
string output_information(string method, string ans, string time) {
    /*
    method 10char
    ans    50char
    time   
    */
   string s;
   s = GREEN + method;
   for(int i = method.size(); i < 10; i++) {
    s += " ";
   }
   s += BLUE + ans;
   for(int i = ans.size(); i < 50; i++) {
    s += " ";
   }
   s += RED + time + "\n\n";
   s += NORMAL;

   return s;
    
}

string output_information(string method, chbench_q1_data &data, string time) {
    /*
    method    time
    data:         number       sum_quantity       sum_amount       avg_quantity       avg_amount 
    */
   string ans;
   string s;
   s =method + "    ";
   s +=time + "\n";
   ans += s;
   for(int j = 0; j < s.size(); j++) {
        ans += " ";
    }
    ans += "number             sum_quantity       sum_amount         avg_quantity       avg_amount\n\n";
   for(int i = 0; i < data.d_size; i++) {
    if(data.cnt[i] == 0) 
        continue;
    for(int j = 0; j < s.size(); j++) {
        ans += " ";
    }
    string tmp = to_string(i);
    ans += tmp;
    for(int j = 0; j <19 - tmp.size(); j++) {
        ans += " ";
    }
    tmp = to_string(data.sum_quantity[i]);
    ans += tmp;
    for(int j = 0; j <19 - tmp.size(); j++) {
        ans += " ";
    }
    tmp = to_string(data.sum_amount[i]);
    ans += tmp;
    for(int j = 0; j <19 - tmp.size(); j++) {
        ans += " ";
    }
    tmp = to_string(data.avg_quantity[i]);
    ans += tmp;
    for(int j = 0; j <19 - tmp.size(); j++) {
        ans += " ";
    }
    tmp = to_string(data.avg_amount[i]);
    ans += tmp;
    for(int j = 0; j <19 - tmp.size(); j++) {
        ans += " ";
    }
    ans += to_string(data.cnt[i]);
    ans += "\n\n";
   }

   return ans;
    
}