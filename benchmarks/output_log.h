#ifndef _OUTPUTLOG_H_
#define _OUTPUTLOG_H_

#include<string>
#include "chbench.h"
#define GREEN "\033[32m"
#define NORMAL "\033[0m"
#define BLUE "\033[34m"
#define RED "\033[31m"
std::string output_information(std::string method, std::string ans, std::string time);
std::string output_information(std::string method, chbench_q1_data &data, std::string time);

#endif