# performance_utils
# used in ../performance

web_general_dimension = ['date', 'country', 'url']
web_specific_dimension = ['app_name', 'app_version']
web_metrics = ['avg_time', 'line95', 'success_times', 'failed_times']

# used to filter the outliers
# choose the threshold as almost 50-100 times the largest line95
TIME_THRESHOLD = 1e6


def get_avg_line95_count(nums):
    if len(nums) == 0:
        raise Exception('Empty list when calling function: get_avg_line95_count')

    count_num = len(nums)
    avg = float(sum(nums)) / count_num
    nums.sort(reverse=True)
    line95 = nums[int(count_num * 0.05)]
    return avg, line95, count_num


def rearrange(record):
    try:
        app_name = record['app_name']
        app_version = record['app_version']
        date = record['time'][:8]  # example 20170620233150
        country = record['ip_location']
        url = record['url']
        result = record['result']
        if record['load_time'] and int(record['load_time']) > 0:
            load_time = int(record['load_time'])
        else:
            load_time = 0

        # filter outlier
        if load_time > TIME_THRESHOLD:
            return None
    except Exception as e:
        return None

    cur_key = (date, country, url, app_name, app_version)
    cur_value = {'SUCCESS': [], 'FAILED': 0}
    if result == 'SUCCESS':
        cur_value['SUCCESS'] = [load_time]
    elif result == 'FAILED':
        cur_value['FAILED'] = 1
    else:
        return None

    return (cur_key, cur_value)


def add_times(d1, d2):
    for key in d1:
        d1[key] = d1[key] + d2[key]
    return d1


def recol(record):
    dims = record[0]
    avg, line95, success_times = get_avg_line95_count(record[1]['SUCCESS'])
    failed_times = record[1]['FAILED']
    return list(dims) + [avg, line95, success_times, failed_times]


def general_map(record):
    cur_key = record[0][:len(web_general_dimension)] + tuple(['all'] * len(web_specific_dimension))
    cur_value = record[1]
    return (cur_key, cur_value)


def get_web_performance_report_df(specific_rdd, action, spark):
    from pyspark.sql.functions import lit

    specific_rdd = specific_rdd.map(rearrange) \
                            .filter(lambda x: x) \
                            .reduceByKey(add_times) \
                            .filter(lambda x: len(x[1]['SUCCESS']) > 0) \
                            .cache()

    general_rdd = specific_rdd.map(general_map) \
                            .reduceByKey(add_times) \
                            .map(recol)

    specific_rdd = specific_rdd.map(recol)

    report = general_rdd.union(specific_rdd)
    report = spark.createDataFrame(report, schema=web_general_dimension + web_specific_dimension + web_metrics) \
                .withColumn('action', lit(action))
    return report
