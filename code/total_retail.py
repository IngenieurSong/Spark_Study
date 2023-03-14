from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("total_retail")
sc = SparkContext(conf = conf)

def parsing_data(x):
    x = x.split(',')
    customer_id = x[0]
    price = float(x[2])
    return (customer_id, price)

# customer-orders.csv에 대한 rdd 생성
lines = sc.textFile("File:///SparkCourse/customer-orders.csv")
# customer_id와 소비량만 있는 rdd 변환
parsedData = lines.map(parsing_data)
# 같은 customer_id를 가진 고객의 소비량을 더하고, 오름차순 정령
total_retail = parsedData.reduceByKey(lambda x, y : x + y).sortBy(lambda x : x[1])
# print를 위해서 collect
result = total_retail.collect()

for c, p in result:
    print(c, p)
