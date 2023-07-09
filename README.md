##Demo Model pipeline dùng pyspark
- Tập dữ liệu đã sử dụng: 
https://www.kaggle.com/code/miquar/explore-flights-csv-airports-csv-airlines-csv/input?select=flights.csv
- Tổng quan về tập dữ liệu:
Thông tin về số chuyến bay đúng giờ, bị chậm, bị hủy và chuyển hướng được công bố trong Báo cáo Người tiêu dùng Du lịch Hàng không hàng tháng của DOT và trong bộ dữ liệu về các chuyến bay bị hoãn và hủy chuyến năm 2015 này.
- Các cột trong dataset
 
Ý nghĩa các trường như sau:
•	YEAR: string (nullable = true): Năm.
•	MONTH: string (nullable = true): Tháng.
•	DAY: string (nullable = true): Ngày.
•	DAY_OF_WEEK: string (nullable = true): Ngày trong tuần.
•	AIRLINE: string (nullable = true): Hãng hàng không.
•	FLIGHT_NUMBER: string (nullable = true): Mã chuyến bay.
•	TAIL_NUMBER: string (nullable = true): Số hiệu máy bay.
•	ORIGIN_AIRPORT: string (nullable = true): Nơi xuất phát.
•	DESTINATION_AIRPORT: string (nullable = true): Điểm đến.
•	SCHEDULED_DEPARTURE: string (nullable = true): Lịch trình xuất phát.
•	DEPARTURE_TIME: string (nullable = true): Thời gian xuất phát thực tế.
•	DEPARTURE_DELAY: string (nullable = true): Thời gian bị trễ.
•	TAXI_OUT: string (nullable = true): Thời gian taxi ra.
•	WHEELS_OFF: string (nullable = true): Thời gian lăn bánh cất cánh.
•	SCHEDULED_TIME: string (nullable = true): Thời gian theo lịch trình.
•	ELAPSED_TIME: string (nullable = true): Không rõ.
•	AIR_TIME: string (nullable = true): Thời gian cất cánh.
•	DISTANCE: string (nullable = true): Khoảng cách.
•	WHEELS_ON: string (nullable = true): Thời gian lăn bánh hạ cánh.
•	TAXI_IN: string (nullable = true): thời gian taxi vào
•	SCHEDULED_ARRIVAL: string (nullable = true): Thời gian theo lịch di chuyển.
•	ARRIVAL_TIME: string (nullable = true): Thời gian di chuyển.
•	ARRIVAL_DELAY: string (nullable = true): Thời gian trễ.
•	DIVERTED: string (nullable = true): Chuyển hướng.
•	CANCELLED: string (nullable = true): Hủy chuyến.
•	CANCELLATION_REASON: string (nullable = true): Lý do hủy.
•	AIR_SYSTEM_DELAY: string (nullable = true): Trễ vì hệ thống hàng không.
•	SECURITY_DELAY: string (nullable = true): Trễ vì lý do an ninh.
•	AIRLINE_DELAY: string (nullable = true): Trễ vì lý do từ hãng.
•	LATE_AIRCRAFT_DELAY: string (nullable = true): Trễ vì phi cơ.
•	WEATHER_DELAY: string (nullable = true): Trễ vì thời tiết.
•	HOUR_ARR: double (nullable = true): Số h di chuyển.
