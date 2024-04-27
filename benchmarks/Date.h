#include<assert.h>
#include <iostream>
#include <random>
using namespace std;

class Date
{
public:
    Date(int year = 1,int month = 1,int day = 1)
    {
        if (year >= 1800 && month > 0 && month<13 && day>0 && day <= GetMonthDay(year, month))
        {
            _year = year;
            _month = month;
            _day = day;
        }
        else
        {
            cout << "wrong date" << endl;
            assert(false);
        }
    }

    Date(const Date& d)
    {
        _year = d._year;
        _month = d._month;
        _day = d._day;
    }


    bool operator==(const Date &d) const
    {
        return (_year == d._year) && (_month == d._month) && (_day == d._day);
    }

    bool operator>(const Date &d) const
    {
        if (_year > d._year)
        {
            return true;
        }
        else if (_year == d._year&&_month > d._month)
        {
            return true;
        }
        else if (_year == d._year&&_month == d._month&&_day > d._day)
        {
            return true;
        }
        else
            return false;
    }

    bool operator>=(const Date &d) const
    {
        return (*this > d) || (*this == d);
    }

    bool operator<(const Date &d) const
    {
        return !(*this >= d);
    }
    bool operator<=(const Date &d) const
    {
        return !(*this > d);
    }


    Date operator=(const Date &d)
    {
        if (this != &d)
        {
            this->_year = d._year;
            this->_month = d._month;
            this->_day = d._day;
        }
        return *this;
    }

private:
    bool IsLeapYear(int year)
    {
        if (((year % 4 == 0) && (year % 100 != 0)) || (year % 400 == 0))
            return true;
        else
            return false;
    }

    int GetMonthDay(int year, int month)
    {
        int monthArray[13] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

        int day = monthArray[month];

        if (month == 2 && IsLeapYear(year))
        {
            day += 1;
        }

        return day;
    }

public:


    Date & operator++()
    {
        ++_day;
        if (_day > GetMonthDay(_year, _month))
        {
            _day -= GetMonthDay(_year, _month);
            assert(_day == 1);
            ++_month;
            if (_month > 12)
            {
                ++_year;
                _month = 1;
            }
        }
        return *this;
    }

    Date operator++(int)
    {
        Date tmp = *this;
        *this = operator++();
        return tmp;
    }

    Date & operator--()
    {
        if (_day > 1)
        {
            --_day;
        }
        else
        {
            if (_month == 1)
            {
                --_year;
                _month = 12;
                _day = GetMonthDay(_year, _month);
            }
            else
            {
                --_month;
                _day = GetMonthDay(_year, _month);
            }
        }
        return *this;
    }

    Date operator--(int)
    {
        Date tmp = *this;
        *this = operator--();
        return tmp;
    }

    int operator-(Date & d)
    {
        if (_year <d. _year)
        {
            Date tmp =* this;
            *this = d;
            d = tmp;
        }
        else if (_year == d._year&&_month < d._month)
        {
            Date tmp = *this;
            *this = d;
            d = tmp;
        }
        else if (_year ==d. _year&&_month == d._month&&_day < d._day)
        {
            Date tmp = *this;
            *this = d;
            d = tmp;
        }
            Date tmp1(*this);
            Date tmp2(d);
            int ret = 0;
            while (!(tmp2 == tmp1))
            {
                tmp2.operator++();
                ret++;
            }
            return ret;
    }

    Date URandDate(Date& l, mt19937_64 &rng)
    {
        // date to timestamp
        tm r_time = {0};
        r_time.tm_year = _year - 1900;
        r_time.tm_mon = _month - 1;
        r_time.tm_mday = _day;

        tm l_time = {0};
        l_time.tm_year = l._year - 1900;
        l_time.tm_mon = l._month - 1;
        l_time.tm_mday = l._day;

        time_t r_ts, l_ts;
        r_ts = mktime(&r_time);
        l_ts = mktime(&l_time);

        // rand timestamp
        uniform_int_distribution<time_t> distribution(l_ts, r_ts);
        time_t rand_ts =distribution(rng);

        // timestamp to date
        tm rand_time;
        localtime_r(&rand_ts, &rand_time);
        Date ret(rand_time.tm_year + 1900, rand_time.tm_mon + 1, rand_time.tm_mday);
        assert(l <= *this);
        return ret;
    }

    uint64_t DateToUint64()
    {
        return (uint64_t)_year * 10000 + _month * 100 + _day;
    }

    int _year;
    int _month;
    int _day;
    time_t ts;
};
