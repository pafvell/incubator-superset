import math
import numpy as np
import scipy
from scipy.optimize import curve_fit
from pyspark.sql import functions as func
from pyspark.sql.functions import col, row_number, udf, count, collect_list, lit
from pyspark.sql.types import StringType, IntegerType, DoubleType, ArrayType, BooleanType

import sys, os
import re
import logging
sys.path.append(os.path.join(os.path.dirname(__file__), '../utils'))
import ime_utils
from datetime import date, datetime, timedelta
from collections import OrderedDict

roi_channel_min_pt_nb = 6
ECPMDOWN_START_DATE = '20180701'

def fitfunc(x, a, b):
    y = a * scipy.power(x, b)
    return y


def reject_outliers(data, m=2):
    return data[abs(data - np.mean(data)) <= m * np.std(data)]


def sort_list_by_n(n, lists):
    len_n = len(n)
    # first to store n, others for sorted lists
    res = []
    for l in [n] + lists:
        if len(l) != len_n:
            raise Exception('lists dont have same size')
        else:
            res.append([])
    merged_list = zip(*([n] + lists))
    merged_list.sort(key=lambda tup: tup[0])
    for item in merged_list:
        for idx, v in enumerate(item):
            res[idx].append(v)
    return res[0], res[1:]


def sort_list(line):
    newline = {}
    list_keys = ime_utils.format_roi_keys(line.asDict().keys(), [], ['n_list', 'activate_list', 'retention_list', 'revenue_list', 'before_revenue_list'])

    for k in list_keys:
        newline[k] = line[k]

    lists = {}
    for l in ['activate_list', 'retention_list', 'revenue_list', 'before_revenue_list']:
        if l in line.asDict().keys():
            lists[l] = line[l]
    # avoid disordered list in cluster mode
    n, new_lists = sort_list_by_n(line['n_list'], lists.values())
    newline['n_list'] = n
    for k, v in enumerate(lists.items()):
        newline[v[0]] = new_lists[k]

    return newline


def calculate_ltvgroup(arpdaugroup, ltgroup, n):
    if not ltgroup:
        raise Exception('ltgroup None')
    if not arpdaugroup:
        raise Exception('arpdaugroup None')
    assert(len(ltgroup) == len(arpdaugroup))

    ltvgroup = [float(ltgroup[i]) * float(arpdaugroup[i]) for i in range(len(ltgroup))]
    return ltvgroup


def calculate_arpdau(revenue, retention, n):
    sum_retention = sum(retention[1:])
    arpdau = sum(revenue[1:]) / sum_retention if sum_retention > 0 else 0.0
    return arpdau


def summary(group, day):
    return sum(group[:day]) if len(group) != 0 else 0.0


def gen_ecpm(tag=''):
    ret_ecpm_variance = OrderedDict()
    start_date = '20170101'
    start_time = datetime.strptime(start_date, '%Y%m%d')
    ecpm_list = {
        '': 1.0,
        '_ecpmdown_20': 0.8,
        '_ecpmdown_30': 0.7,
        '_ecpmdown_40': 1,
        '_ecpmdown_50': 0.5,
        '_ecpmdown_60': 0.4,
    }
    ecpm_drop = ecpm_list[tag]
    for d in range(1500):
        base_ecpm_var = 1.0
        this_time = (start_time + timedelta(d))
        if tag == '_ecpmdown_40':
            this_time_str = datetime.strftime(this_time, '%m%d')
            if this_time_str in ['1225', '1226', '1227', '1228', '1229', '1230', '1231']:
                base_ecpm_var = 0.6
            elif this_time_str in ['0101', '0102', '0103', '0104', '0105', '0106', '0107']:
                base_ecpm_var = 0.54
        if this_time.strftime('%Y%m%d') >= ECPMDOWN_START_DATE:
            base_ecpm_var *= ecpm_drop
        ret_ecpm_variance[this_time.strftime('%Y%m%d')] = base_ecpm_var
    return ret_ecpm_variance


class LTV(object):
    # activate_threshold : minimum value of activation number for a day to be considered as valid in prediction
    # retention_threshold : minimum value of retention number for a day to be considered as valid in prediction
    # min_pt_nb : minimum number of point used in curve fitting
    # real_revenue_days : number of days with all activities already happened
    def __init__(self, dim, days=[90], activate_threshold=100, retention_threshold=10, min_pt_nb=3, real_revenue_days=2, tag=''):
        max_day = max(days)
        LT_DAYS_RANGE = range(max_day)
        ecpm = gen_ecpm(tag)

        def merge_real_predict(real, predict, day_idx=None):
            if not day_idx:
                day_idx = min(real_revenue_days, max_day, len(real))
                # if no predict value and day idx is not defined
                # take all real revenue
                if sum(predict) == 0.0:
                    day_idx = min(len(real), max_day)

            return real[:day_idx] + predict[day_idx:]

        def predict_life_time(x, y, full_y):
            # information on first day is unstable, exclude in curve fit
            logx = [math.log(it - 1) for it in x[1:]]
            logy = [math.log(it) for it in y[1:]]
            tmp_res = [0.0 for i in LT_DAYS_RANGE]
            if len(logx) >= min_pt_nb:
                p = np.polyfit(logx, logy, 1)
                b = p[0]
                a = math.exp(p[1])
                # days from 3 .. 90
                # lt should be decreasing
                if b < 0:
                    # 0^b when b < 0 raise math domain error
                    tmp_res = [0.0] + [a * math.pow(day, b) for day in LT_DAYS_RANGE[1:]]

            res = merge_real_predict(full_y, tmp_res)
            return res

        def calculate_ltgroup(activate, retention, n):
            res_list = [0.0 for i in LT_DAYS_RANGE]
            ratio_list = []
            day_list = []
            filter_ratio_list = []
            for i in range(len(activate)):
                ratio_list.append(float(retention[i]) / float(activate[i]))
                if float(activate[i]) >= activate_threshold and float(retention[i]) >= retention_threshold:
                    filter_ratio_list.append(float(retention[i]) / float(activate[i]))
                    day_list.append(n[i])
            res_list = predict_life_time(day_list, filter_ratio_list, ratio_list)
            return res_list

        def join_ecpm_variance(arpdaugroup, end_date):
            ret_arpdaugroup = []
            end_time = datetime.strptime(str(end_date), '%Y%m%d')
            predict_dates = [(end_time + timedelta(i)).strftime('%Y%m%d') for i in range(len(arpdaugroup))]
            for idx, d in enumerate(predict_dates):
                ret_arpdaugroup.append(arpdaugroup[idx] * ecpm[d])
            return ret_arpdaugroup

        def calculate_arpdaugroup(revenue, retention, n, app_name, end_act_date, before_revenue):
            arpdau = [0.0 for i in LT_DAYS_RANGE]
            arpdau_list = [(revenue[i] / retention[i] if retention[i] > 0 else 0.0) for i in range(len(n))]
            before_arpdau_list = [(before_revenue[i] / retention[i] if retention[i] > 0 else 0.0) for i in range(len(n))]

            filter_n_list = []
            filter_arpdau_list = []
            for idx in range(len(n)):
                # not include first days' revenue in arpdau estimation, cause of stability
                # filter days with very low retention
                if idx > 0 and retention[idx] >= retention_threshold:
                    filter_n_list.append(n[idx] - 1)
                    filter_arpdau_list.append(arpdau_list[idx])

            popt = [0, 0]
            # fit arpdau curve for all products
            if len(filter_n_list) >= min_pt_nb:
                try:
                    # use curve fit because arpdau can be 0
                    popt, pcov = curve_fit(fitfunc, filter_n_list, filter_arpdau_list, maxfev=2000)
                except RuntimeError as re:
                    logging.info('unable to fit curve with %s %s' % (str(n), str(arpdau_list)))

            # y = a*x^b with coef b in interval [-1, 0) arpdau is not incremental
            if (popt[1] < 0) and (popt[1] >= -1):
                arpdau = merge_real_predict(before_arpdau_list, join_ecpm_variance([0.0] + fitfunc(LT_DAYS_RANGE[1:], popt[0], popt[1]).tolist(), end_act_date))
            elif len(filter_arpdau_list) > 1:
                # if out of the interval than use average
                # prevent increasing arpdau
                avg_arpdau = np.mean(reject_outliers(np.array(filter_arpdau_list))).item()
                arpdau = merge_real_predict(before_arpdau_list, join_ecpm_variance([avg_arpdau for i in LT_DAYS_RANGE], end_act_date))
            else:
                arpdau = merge_real_predict(before_arpdau_list, [0.0 for i in LT_DAYS_RANGE])

            return arpdau

        def deweighted_revenue(revenue, end_date, n):
            ecpm_var = ecpm[(datetime.strptime(end_date, '%Y%m%d') + timedelta(n - 1)).strftime('%Y%m%d')]
            return revenue / ecpm_var

        self.dim = dim
        self.days = days
        self.tag = tag
        self.udf_arpdaugroup = udf(calculate_arpdaugroup, ArrayType(DoubleType()))
        self.udf_arpdau = udf(lambda rev, atv, n: calculate_arpdau(rev, atv, n), DoubleType())
        self.udf_lt = udf(lambda a, r, n: calculate_ltgroup(a, r, n), ArrayType(DoubleType()))
        self.udf_ltv = udf(calculate_ltvgroup, ArrayType(DoubleType()))
        self.udf_sum = udf(lambda gp, day: summary(gp, day), DoubleType())
        self.udf_is_predict = udf(lambda x: (x[-1] > 0) if len(x) > 0 else False, BooleanType())
        self.udf_max = udf(lambda t: max(t), IntegerType())
        self.udf_deweighted_revenue = udf(deweighted_revenue, DoubleType())

    def __check_schema__(self, retention=None, revenue=None):
        dim = self.dim

        if retention:
            assert(all(x in retention.schema.names for x in dim + ['act_date', 'n', 'activate', 'retention', 'app_name']))
        if revenue:
            assert(all(x in revenue.schema.names for x in dim + ['act_date', 'n', 'revenue', 'app_name']))

    def predict_ltv(self, retention, revenue):
        self.__check_schema__(retention=retention, revenue=revenue)
        dim = self.dim
        days = self.days
        tag = self.tag

        ltv = self.__predict_ltvgroup__(retention, revenue)
        for day in days:
            ltv = ltv.withColumn('lt_%s%s' % (day, tag), self.udf_sum(col('ltgroup%s' % tag), lit(day)))\
                .withColumn('ltv_%s%s' % (day, tag), self.udf_sum(col('ltvgroup%s' % tag), lit(day)))

        ltv = ltv.withColumn('retention_nb', self.udf_max(col('n_list')))\
            .withColumn('activate', self.udf_max(col('activate_list')))\
            .withColumn('is_predict%s' % tag, self.udf_is_predict(col('ltgroup%s' % tag)))

        return ltv

    def fill_missing_columns(self, df, dims):
        if 'country' in dims:
            df = df.fillna('-', 'country')
        return df.fillna('none', dims)

    def predict_lt(self, retention):
        self.__check_schema__(retention=retention)
        dim = self.dim
        days = self.days
        tag = self.tag

        lt = self.__predict_ltgroup__(retention)
        for day in days:
            lt = lt.withColumn('lt_%s%s' % (day, tag), self.udf_sum(col('ltgroup%s' % tag), lit(day)))

        lt = lt.withColumn('is_predict%s' % (tag), self.udf_is_predict(col('ltgroup%s' % tag)))
        return lt

    def __predict_arpdaugroup__(self, retention, revenue):
        self.__check_schema__(retention=retention, revenue=revenue)
        dim = self.dim
        tag = self.tag
        arpdau = self.fill_missing_columns(retention, dim + ['act_date', 'n'])\
            .join(self.fill_missing_columns(revenue, dim + ['act_date', 'n']), on=dim + ['act_date', 'n'], how='left_outer')\
            .fillna(0.0, 'revenue')\
            .withColumnRenamed('revenue', 'before_revenue')\
            .withColumn('revenue', self.udf_deweighted_revenue(col('before_revenue'), col('act_date'), col('n'))).groupBy(dim + ['n'])\
            .agg(func.sum('retention').alias('retention'), func.sum('revenue').alias('revenue'), func.sum('before_revenue').alias('before_revenue'),
                 func.min('act_date').alias('start_act_date'), func.max('act_date').alias('end_act_date'))\
            .orderBy('n').groupBy(dim)\
            .agg(collect_list('retention').alias('retention_list'), collect_list('revenue').alias('revenue_list'), collect_list('n').alias('n_list'),
                 collect_list('before_revenue').alias('before_revenue_list'), func.min('start_act_date').alias('start_act_date'), func.max('end_act_date').alias('end_act_date'))\
            .rdd.map(sort_list).toDF()\
            .withColumn('arpdaugroup%s' % tag, self.udf_arpdaugroup(col('revenue_list'), col('retention_list'), col('n_list'), col('app_name'),
                                                                    col('end_act_date'), col('before_revenue_list')))\
            .withColumn('arpdau%s' % tag, self.udf_arpdau(col('before_revenue_list'), col('retention_list'), col('n_list')))
        return arpdau

    def __predict_ltgroup__(self, retention):
        dim = self.dim
        tag = self.tag

        ltgroup = retention.groupBy(dim + ['n'])\
            .agg(func.sum('activate').alias('activate'), func.sum('retention').alias('retention')\
                , func.min('act_date').alias('start_date'), func.max('act_date').alias('end_date'))\
            .orderBy('n').groupBy(dim)\
            .agg(collect_list('activate').alias('activate_list'), collect_list('retention').alias('retention_list')\
                , collect_list('n').alias('n_list'), func.min('start_date').alias('start_date')\
                , func.max('end_date').alias('end_date'))\
            .rdd.map(sort_list).toDF()\
            .withColumn('ltgroup%s' % tag, self.udf_lt(col('activate_list'), col('retention_list'), col('n_list')))
        return ltgroup

    def __predict_ltvgroup__(self, retention, revenue):
        dim = self.dim
        tag = self.tag

        ltgroup = self.__predict_ltgroup__(retention)
        arpdau = self.__predict_arpdaugroup__(retention, revenue).drop('n_list', 'retention_list')

        ltvgroup = self.fill_missing_columns(ltgroup, dim).join(arpdau, on=dim, how='left_outer')\
            .withColumn('ltvgroup%s' % tag, self.udf_ltv(col('arpdaugroup%s' % tag), col('ltgroup%s' % tag), col('n_list')))\
            .drop('arpdaugroup%s' % tag)

        return ltvgroup
