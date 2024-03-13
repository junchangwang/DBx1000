#include<iostream>
#include<string>
#include "output_log.h"
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