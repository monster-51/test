import jaydebeapi
import os


jdbc_driver_jar = "clickhouse-jdbc-0.2.6.jar"  
jdbc_url = "jdbc:clickhouse://localhost:8123/default"
jdbc_driver_class = "ru.yandex.clickhouse.ClickHouseDriver"
jdbc_driver_args = ["", ""]


os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"


conn = jaydebeapi.connect(jdbc_driver_class, jdbc_url, jdbc_driver_args, jdbc_driver_jar)




cursor = conn.cursor()


cursor.execute("SELECT * FROM your_table")


results = cursor.fetchall()


for row in results:
    print(row)


cursor.close()
conn.close()