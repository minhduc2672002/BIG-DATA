## Demo Model pipeline dùng pyspark
- Tập dữ liệu đã sử dụng: 
https://www.kaggle.com/code/miquar/explore-flights-csv-airports-csv-airlines-csv/input?select=flights.csv
- Tổng quan về tập dữ liệu:
Thông tin về số chuyến bay đúng giờ, bị chậm, bị hủy và chuyển hướng được công bố trong Báo cáo Người tiêu dùng Du lịch Hàng không hàng tháng của DOT và trong bộ dữ liệu về các chuyến bay bị hoãn và hủy chuyến năm 2015 này.
- Các cột trong dataset
 
Ý nghĩa các trường như sau:
* YEAR: string (nullable = true): Năm.
*	MONTH: string (nullable = true): Tháng.
*	DAY: string (nullable = true): Ngày.
*	DAY_OF_WEEK: string (nullable = true): Ngày trong tuần.
*	AIRLINE: string (nullable = true): Hãng hàng không.
*	FLIGHT_NUMBER: string (nullable = true): Mã chuyến bay.
*	TAIL_NUMBER: string (nullable = true): Số hiệu máy bay.
*	ORIGIN_AIRPORT: string (nullable = true): Nơi xuất phát.
*	DESTINATION_AIRPORT: string (nullable = true): Điểm đến.
*	SCHEDULED_DEPARTURE: string (nullable = true): Lịch trình xuất phát.
*	DEPARTURE_TIME: string (nullable = true): Thời gian xuất phát thực tế.
*	DEPARTURE_DELAY: string (nullable = true): Thời gian bị trễ.
*	TAXI_OUT: string (nullable = true): Thời gian taxi ra.
*	WHEELS_OFF: string (nullable = true): Thời gian lăn bánh cất cánh.
*	SCHEDULED_TIME: string (nullable = true): Thời gian theo lịch trình.
*	ELAPSED_TIME: string (nullable = true): Không rõ.
*	AIR_TIME: string (nullable = true): Thời gian cất cánh.
*	DISTANCE: string (nullable = true): Khoảng cách.
*	WHEELS_ON: string (nullable = true): Thời gian lăn bánh hạ cánh.
*	TAXI_IN: string (nullable = true): thời gian taxi vào
*	SCHEDULED_ARRIVAL: string (nullable = true): Thời gian theo lịch di chuyển.
*	ARRIVAL_TIME: string (nullable = true): Thời gian di chuyển.
*	ARRIVAL_DELAY: string (nullable = true): Thời gian trễ.
*	DIVERTED: string (nullable = true): Chuyển hướng.
*	CANCELLED: string (nullable = true): Hủy chuyến.
*	CANCELLATION_REASON: string (nullable = true): Lý do hủy.
*	AIR_SYSTEM_DELAY: string (nullable = true): Trễ vì hệ thống hàng không.
*	SECURITY_DELAY: string (nullable = true): Trễ vì lý do an ninh.
*	AIRLINE_DELAY: string (nullable = true): Trễ vì lý do từ hãng.
*	LATE_AIRCRAFT_DELAY: string (nullable = true): Trễ vì phi cơ.
*	WEATHER_DELAY: string (nullable = true): Trễ vì thời tiết.
•	HOUR_ARR: double (nullable = true): Số h di chuyển.




    
Khởi tạo một connection: Trong bài này do mới làm quen với spark nên
mình sẽ khởi tạo một cluster trên local. Thay vì kết nối tới những máy
khác, các tính toán sẽ được thực hiện chỉ trên server local thông qua
một giả lập cụm.
  

   
``` python
from pyspark import SparkContext
# Stop spark if it existed.
try:
    sc.stop()
except:
    print('sc have not yet created!')
    
sc = SparkContext(master = "local", appName = "First app")
# Check spark context
print(sc)
# Check spark context version
print(sc.version)
```

    
chúng ta chỉ có thể khởi tạo SparkContext 1 lần nên nếu chưa stop sc mà
chạy lại lệnh trên sẽ bị lỗi. Do đó lệnh SparkContext.getOrCreate() được
sử dụng để khởi tạo mới SparkContext nếu nó chưa xuất hiện và lấy lại
SparkContext cũ nếu đã được khởi tạo và đang run.
  

  
``` python
sc = SparkContext.getOrCreate()
print(sc)
```

   <SparkContext master=local appName=First app>
  
  

    
Khởi tạo một Session trong SparkContext:
  

``` python
from pyspark.sql import SparkSession
my_spark = SparkSession.builder.getOrCreate()
# Print my_spark session
print(my_spark)
```

   <pyspark.sql.session.SparkSession object at 0x00000279D70CEAD0>
  
  

    
Thuộc tính catalog trong 1 Session sẽ liệt kê toàn bộ các dữ liệu có bên
trong 1 cluster. Trong đó để xem toàn bộ các bảng đang có trong cluster
chúng ta sử dụng hàm .listTables().
  

``` python
# List all table exist in spark sesion
my_spark.catalog.listTables()
```


    
Hiện tại trên cluster đang chưa có bảng nào. Để thêm một bảng vào
cluster chúng ta có thể đọc từ my_spark.read.csv() Dữ liệu file csv
  

``` python
flights = my_spark.read.csv('./flights.csv', header = True)
# show fligths top 5
flights.show(5)
```

  
 
    
Để kiểm tra các schema có trong một table chúng ta sử dụng hàm
printSchema().
  

``` python
flights.printSchema()
```

    root
     |-- YEAR: string (nullable = true)
     |-- MONTH: string (nullable = true)
     |-- DAY: string (nullable = true)
     |-- DAY_OF_WEEK: string (nullable = true)
     |-- AIRLINE: string (nullable = true)
     |-- FLIGHT_NUMBER: string (nullable = true)
     |-- TAIL_NUMBER: string (nullable = true)
     |-- ORIGIN_AIRPORT: string (nullable = true)
     |-- DESTINATION_AIRPORT: string (nullable = true)
     |-- SCHEDULED_DEPARTURE: string (nullable = true)
     |-- DEPARTURE_TIME: string (nullable = true)
     |-- DEPARTURE_DELAY: string (nullable = true)
     |-- TAXI_OUT: string (nullable = true)
     |-- WHEELS_OFF: string (nullable = true)
     |-- SCHEDULED_TIME: string (nullable = true)
     |-- ELAPSED_TIME: string (nullable = true)
     |-- AIR_TIME: string (nullable = true)
     |-- DISTANCE: string (nullable = true)
     |-- WHEELS_ON: string (nullable = true)
     |-- TAXI_IN: string (nullable = true)
     |-- SCHEDULED_ARRIVAL: string (nullable = true)
     |-- ARRIVAL_TIME: string (nullable = true)
     |-- ARRIVAL_DELAY: string (nullable = true)
     |-- DIVERTED: string (nullable = true)
     |-- CANCELLED: string (nullable = true)
     |-- CANCELLATION_REASON: string (nullable = true)
     |-- AIR_SYSTEM_DELAY: string (nullable = true)
     |-- SECURITY_DELAY: string (nullable = true)
     |-- AIRLINE_DELAY: string (nullable = true)
     |-- LATE_AIRCRAFT_DELAY: string (nullable = true)
     |-- WEATHER_DELAY: string (nullable = true)
  
  

    
Thêm một spark DataFrame lưu trữ local vào một catalog Tuy nhiên lúc này
flights vẫn chỉ là một spark DataFrame chưa có trong catalog của của
cluster. Sử dụng hàm listTable() liệt kê danh sách bảng ta thu được 1
list rỗng.
  

``` python
print(my_spark.catalog.listTables())
```

    []
  
  

``` python
# Create a temporary table on catalog of local data frame flights as new temporary table flights_temp on catalog
flights.createOrReplaceTempView('flights_temp')
# check list all table available on catalog
my_spark.catalog.listTables()
```
    
Ta sẽ thêm 1 trường mới là HOUR_ARR được tính dựa trên AIR_TIME/60 (qui
từ phút ra h) của bảng flights như sau:
  

``` python
flights = flights.withColumn('HOUR_ARR', flights.AIR_TIME/60)
flights.printSchema()
```

    root
     |-- YEAR: string (nullable = true)
     |-- MONTH: string (nullable = true)
     |-- DAY: string (nullable = true)
     |-- DAY_OF_WEEK: string (nullable = true)
     |-- AIRLINE: string (nullable = true)
     |-- FLIGHT_NUMBER: string (nullable = true)
     |-- TAIL_NUMBER: string (nullable = true)
     |-- ORIGIN_AIRPORT: string (nullable = true)
     |-- DESTINATION_AIRPORT: string (nullable = true)
     |-- SCHEDULED_DEPARTURE: string (nullable = true)
     |-- DEPARTURE_TIME: string (nullable = true)
     |-- DEPARTURE_DELAY: string (nullable = true)
     |-- TAXI_OUT: string (nullable = true)
     |-- WHEELS_OFF: string (nullable = true)
     |-- SCHEDULED_TIME: string (nullable = true)
     |-- ELAPSED_TIME: string (nullable = true)
     |-- AIR_TIME: string (nullable = true)
     |-- DISTANCE: string (nullable = true)
     |-- WHEELS_ON: string (nullable = true)
     |-- TAXI_IN: string (nullable = true)
     |-- SCHEDULED_ARRIVAL: string (nullable = true)
     |-- ARRIVAL_TIME: string (nullable = true)
     |-- ARRIVAL_DELAY: string (nullable = true)
     |-- DIVERTED: string (nullable = true)
     |-- CANCELLED: string (nullable = true)
     |-- CANCELLATION_REASON: string (nullable = true)
     |-- AIR_SYSTEM_DELAY: string (nullable = true)
     |-- SECURITY_DELAY: string (nullable = true)
     |-- AIRLINE_DELAY: string (nullable = true)
     |-- LATE_AIRCRAFT_DELAY: string (nullable = true)
     |-- WEATHER_DELAY: string (nullable = true)
     |-- HOUR_ARR: double (nullable = true)
  
  

    
Bên dưới chúng ta sẽ tạo ra trường avg_speed tính vận tốc trung bình của
các máy bay bằng cách lấy khoảng cách (DISTANCE) chi cho thời gian bay
(HOUR_ARR) group by theo mã máy bay (TAIL_NUMBER) bằng lệnh select.
  

``` python
avg_speed = flights.select("ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "TAIL_NUMBER", (flights.DISTANCE/flights.HOUR_ARR).alias("avg_speed"))
avg_speed.printSchema()
avg_speed.show(5)
```

    root
     |-- ORIGIN_AIRPORT: string (nullable = true)
     |-- DESTINATION_AIRPORT: string (nullable = true)
     |-- TAIL_NUMBER: string (nullable = true)
     |-- avg_speed: double (nullable = true)

    +--------------+-------------------+-----------+-----------------+
    |ORIGIN_AIRPORT|DESTINATION_AIRPORT|TAIL_NUMBER|        avg_speed|
    +--------------+-------------------+-----------+-----------------+
    |           ANC|                SEA|     N407AS|514.0828402366864|
    |           LAX|                PBI|     N3KUAA|531.5589353612166|
    |           SFO|                CLT|     N171US|517.8947368421052|
    |           LAX|                MIA|     N3HYAA|544.6511627906978|
    |           SEA|                ANC|     N527AS|436.5829145728643|
    +--------------+-------------------+-----------+-----------------+
    only showing top 5 rows
  
  

``` python
avg_speed_exp = flights.selectExpr("ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "TAIL_NUMBER", "(DISTANCE/HOUR_ARR) AS avg_speed")
avg_speed_exp.printSchema()
avg_speed_exp.show(5)
```

    root
     |-- ORIGIN_AIRPORT: string (nullable = true)
     |-- DESTINATION_AIRPORT: string (nullable = true)
     |-- TAIL_NUMBER: string (nullable = true)
     |-- avg_speed: double (nullable = true)

    +--------------+-------------------+-----------+-----------------+
    |ORIGIN_AIRPORT|DESTINATION_AIRPORT|TAIL_NUMBER|        avg_speed|
    +--------------+-------------------+-----------+-----------------+
    |           ANC|                SEA|     N407AS|514.0828402366864|
    |           LAX|                PBI|     N3KUAA|531.5589353612166|
    |           SFO|                CLT|     N171US|517.8947368421052|
    |           LAX|                MIA|     N3HYAA|544.6511627906978|
    |           SEA|                ANC|     N527AS|436.5829145728643|
    +--------------+-------------------+-----------+-----------------+
    only showing top 5 rows
  
  

    
lọc những chuyến bay xuất phát từ sân bay SEA và điểm đến là ANC ta có
thể sử dụng filter như sau:
  

``` python
filter_SEA_ANC = flights.filter("ORIGIN_AIRPORT == 'SEA'") \
                        .filter("DESTINATION_AIRPORT == 'ANC'")
filter_SEA_ANC.show(5)
```

  
  

    
chúng ta sẽ tính thời gian bay trung bình theo điểm xuất phát
(ORIGIN_AIRPORT).
  

  
``` python
avg_time_org_airport = flights.groupBy("ORIGIN_AIRPORT").avg("HOUR_ARR")
avg_time_org_airport.show(5)
```

    +--------------+------------------+
    |ORIGIN_AIRPORT|     avg(HOUR_ARR)|
    +--------------+------------------+
    |           BGM| 1.096525096525096|
    |           PSE|3.0352529358626916|
    |           INL|0.5327937649880096|
    |           MSY|1.7156564469514983|
    |           PPG| 5.153144654088051|
    +--------------+------------------+
    only showing top 5 rows
  
  

    
các lệnh biến đổi dữ liệu của spark.sql()
  

   
``` python
flights_10 = my_spark.sql('SELECT * FROM flights_temp WHERE AIR_TIME > 10')
flights_10.show(5)
```

 

    
Chẳng hạn chúng ta muốn tính số phút bay trung bình của mỗi hãng bay
trong năm sẽ sử dụng hàm GROUP BY như sau:
  

   
``` python
print(my_spark.catalog.listTables())
agg_arr_time = my_spark.sql("SELECT ORIGIN_AIRPORT, DESTINATION_AIRPORT, TAIL_NUMBER, MEAN(AIR_TIME) AS avg_speed FROM flights_temp GROUP BY ORIGIN_AIRPORT, DESTINATION_AIRPORT, TAIL_NUMBER")
agg_arr_time.show(5)
```

    [Table(name='flights_temp', database=None, description=None, tableType='TEMPORARY', isTemporary=True)]
    +--------------+-------------------+-----------+------------------+
    |ORIGIN_AIRPORT|DESTINATION_AIRPORT|TAIL_NUMBER|         avg_speed|
    +--------------+-------------------+-----------+------------------+
    |           IAG|                FLL|     N630NK|             158.0|
    |           RIC|                ATL|     N947DN|  75.6470588235294|
    |           EWR|                ATL|     N970AT|108.02777777777777|
    |           MSN|                ORD|     N703SK|              28.0|
    |           AVL|                ATL|     N994AT|              30.5|
    +--------------+-------------------+-----------+------------------+
    only showing top 5 rows
  
  

    
3.Xây dựng pipeline End-to-End model trên pyspark 3.1. Xây dựng pipeline
biến đổi dữ liệu.

pyspark cho phép xây dựng các end-to-end model mà dữ liệu truyền vào là
các raw data và kết quả trả ra là nhãn, xác xuất hoặc giá trị được dự
báo của model. Các end-to-end model này được đi qua một pipe line của
pyspark.ml bao gồm 2 class cơ bản là Transformer cho phép biến đổi dữ
liệu và Estimator ước lượng mô hình dự báo.
  

   
``` python
flights.printSchema()
```

    root
     |-- YEAR: string (nullable = true)
     |-- MONTH: string (nullable = true)
     |-- DAY: string (nullable = true)
     |-- DAY_OF_WEEK: string (nullable = true)
     |-- AIRLINE: string (nullable = true)
     |-- FLIGHT_NUMBER: string (nullable = true)
     |-- TAIL_NUMBER: string (nullable = true)
     |-- ORIGIN_AIRPORT: string (nullable = true)
     |-- DESTINATION_AIRPORT: string (nullable = true)
     |-- SCHEDULED_DEPARTURE: string (nullable = true)
     |-- DEPARTURE_TIME: string (nullable = true)
     |-- DEPARTURE_DELAY: string (nullable = true)
     |-- TAXI_OUT: string (nullable = true)
     |-- WHEELS_OFF: string (nullable = true)
     |-- SCHEDULED_TIME: string (nullable = true)
     |-- ELAPSED_TIME: string (nullable = true)
     |-- AIR_TIME: string (nullable = true)
     |-- DISTANCE: string (nullable = true)
     |-- WHEELS_ON: string (nullable = true)
     |-- TAXI_IN: string (nullable = true)
     |-- SCHEDULED_ARRIVAL: string (nullable = true)
     |-- ARRIVAL_TIME: string (nullable = true)
     |-- ARRIVAL_DELAY: string (nullable = true)
     |-- DIVERTED: string (nullable = true)
     |-- CANCELLED: string (nullable = true)
     |-- CANCELLATION_REASON: string (nullable = true)
     |-- AIR_SYSTEM_DELAY: string (nullable = true)
     |-- SECURITY_DELAY: string (nullable = true)
     |-- AIRLINE_DELAY: string (nullable = true)
     |-- LATE_AIRCRAFT_DELAY: string (nullable = true)
     |-- WEATHER_DELAY: string (nullable = true)
     |-- HOUR_ARR: double (nullable = true)
  
  

   
``` python
print('Shape of previous data: ({}, {})'.format(flights.count(), len(flights.columns)))
flights_SEA = my_spark.sql("select ARRIVAL_DELAY, ARRIVAL_TIME, MONTH, YEAR, DAY_OF_WEEK, DESTINATION_AIRPORT, AIRLINE from flights_temp where ORIGIN_AIRPORT = 'SEA' and AIRLINE in ('DL', 'AA') ")
print('Shape of flights_SEA data: ({}, {})'.format(flights_SEA.count(), len(flights_SEA.columns)))
```

   Shape of previous data: (5819079, 32)
   Shape of flights_SEA data: (19956, 7)
  
  

``` python
# Create boolean variable IS_DELAY variable as Target
flights_SEA = flights_SEA.withColumn("IS_DELAY", flights_SEA.ARRIVAL_DELAY > 0)
# Now Convert Boolean variable into integer
flights_SEA = flights_SEA.withColumn("label", flights_SEA.IS_DELAY.cast("integer"))
# Remove missing value
model_data = flights_SEA.filter("ARRIVAL_DELAY is not null \
                                and ARRIVAL_TIME is not null \
                                and MONTH is not null \
                                and YEAR is not null  \
                                and DAY_OF_WEEK is not null \
                                and DESTINATION_AIRPORT is not null \
                                and AIRLINE is not null")

print('Shape of model_data data: ({}, {})'.format(model_data.count(), len(model_data.columns)))
```

   Shape of model_data data: (19823, 9)
  
  

    
Convert các biến string sang numeric bằng hàm .withColumn()
  

``` python
# ARRIVAL_TIME, MONTH, YEAR, DAY_OF_WEEK
model_data = model_data.withColumn("ARRIVAL_TIME", model_data.ARRIVAL_TIME.cast("integer"))
model_data = model_data.withColumn("MONTH", model_data.MONTH.cast("integer"))
model_data = model_data.withColumn("YEAR", model_data.YEAR.cast("integer"))
model_data = model_data.withColumn("DAY_OF_WEEK", model_data.DAY_OF_WEEK.cast("integer"))
model_data.printSchema()
```

    root
     |-- ARRIVAL_DELAY: string (nullable = true)
     |-- ARRIVAL_TIME: integer (nullable = true)
     |-- MONTH: integer (nullable = true)
     |-- YEAR: integer (nullable = true)
     |-- DAY_OF_WEEK: integer (nullable = true)
     |-- DESTINATION_AIRPORT: string (nullable = true)
     |-- AIRLINE: string (nullable = true)
     |-- IS_DELAY: boolean (nullable = true)
     |-- label: integer (nullable = true)
  
  

    
Biến đổi các biến String bằng StringIndexer và OneHotEncoder.
  

``` python
from pyspark.ml.feature import StringIndexer, OneHotEncoder
# I. With DESTINATION_AIRPORT
# Create StringIndexer
dest_indexer = StringIndexer(inputCol = "DESTINATION_AIRPORT", \
                            outputCol = "DESTINATION_INDEX")

# Create OneHotEncoder
dest_onehot = OneHotEncoder(inputCol = "DESTINATION_INDEX", \
                            outputCol = "DESTINATION_FACT")

# II. With AIRLINE
# Create StringIndexer
airline_indexer = StringIndexer(inputCol = "AIRLINE", \
                                outputCol = "AIRLINE_INDEX")

# Create OneHotEncoder
airline_onehot = OneHotEncoder(inputCol = "AIRLINE_INDEX", \
                              outputCol = "AIRLINE_FACT")
```
  

   
``` python
# Make a VectorAssembler
from pyspark.ml.feature import VectorAssembler
vec_assembler = VectorAssembler(inputCols = ["ARRIVAL_TIME", "MONTH", "YEAR", \
                                            "DAY_OF_WEEK", "DESTINATION_FACT",\
                                            "AIRLINE_FACT"], 
                                outputCol = "features")
```
  

    
Tiếp theo chúng ta sẽ khởi tạo pipeline biến đổi dữ liệu cho model thông
qua class Pipeline của pyspark.ml. Các transformer biến đổi dữ liệu sẽ
được sắp xếp trong 1 list và truyền vào tham số stages như bên dưới.
  

   
``` python
from pyspark.ml import Pipeline

# Make a pipeline
flights_sea_pipe  = Pipeline(stages = [dest_indexer, dest_onehot, airline_indexer, \
                                      airline_onehot, vec_assembler])
```
  

    
3.2. Huấn luyện và đánh giá model. 3.2.1. Phân chia tập train/test.
  

``` python
# create pipe_data from pipeline
pipe_data = flights_sea_pipe.fit(model_data).transform(model_data)
# Split train/test data
train, test = pipe_data.randomSplit([0.8, 0.2])
```
  

    
3.2.2. Huấn luyện và đánh giá model. chúng ta sẽ chọn ra model
LogisticRegression trong ví dụ demo này làm model phân loại.
  

``` python
from pyspark.ml.classification import LogisticRegression

# Create logistic regression
lr = LogisticRegression()
```
  

    
Để đánh giá model chúng ta cần sử dụng các metric như ROC, Accuracy, F1,
precision hoặc recal. Do dữ liệu không có hiện tượng mất cân bằng giữa 2
lớp nên ta sẽ sử dụng ROC làm metric đánh giá model. Trong trường hợp
mẫu mất cân bằng thì các chỉ số F1, precision hoặc recal nên được sử
dụng thay thế vì trong tình huống này ROC, Accuracy thường mặc định là
rất cao. Chúng ta sử dụng class BinaryClassificationEvaluator trong
pyspark.ml.evaluation module để tính toán các metrics đánh giá model.
  

``` python
# Import the evaluation submodule
import pyspark.ml.evaluation as evals

# Create a BinaryClassificationEvaluator
evaluator = evals.BinaryClassificationEvaluator(metricName = "areaUnderROC")
```
  

    
3.2.3. Tuning model thông qua param gridSearch.
  

``` python
# Import the tuning submodule
import pyspark.ml.tuning as tune
import numpy as np

# Create the parameter grid
grid = tune.ParamGridBuilder()

# Add the hyperparameter, we can add more than one hyperparameter
grid = grid.addGrid(lr.regParam, np.arange(0, 0.1, 0.01))

# Build the grid
grid = grid.build()
```
  

    
Sau khi build xong gridSearch chúng ta cần Cross Validate toàn bộ các
model trên tập các tham số khởi tạo ở grid
  

``` python
# Create the CrossValidator
cv = tune.CrossValidator(estimator=lr,
              estimatorParamMaps=grid,
              evaluator=evaluator
              )

# Fit cross validation on models
models = cv.fit(train)
```
  

    
Sau khi tuning xong chúng ta sẽ thu được model tốt nhất thông qua hàm:
  

``` python
best_lr = models.bestModel
best_lr = lr.fit(train)
print(best_lr)
```

   LogisticRegressionModel: uid=LogisticRegression_1dc11dd70194, numClasses=2, numFeatures=29
  
  

    
Lưu ý để dự báo từ các model của pyspark, chúng ta không sử dụng hàm
predict() như thông thường mà sử dụng hàm transform(). Kiểm tra mức độ
chính xác của model trên tập test bằng hàm evaluate().
  

``` python
# Use the model to predict the test set
test_results = best_lr.transform(test)

# Evaluate the predictions
print(evaluator.evaluate(test_results))
```

   0.5920138641503261
  
  

    
Như vậy trên tập test model đạt mức độ chính xác khoảng 59.2%. Phương án
tốt được cân nhắc là thêm biến để cải thiện model.
  


