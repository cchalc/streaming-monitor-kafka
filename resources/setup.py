username = "cchalc"
dbutils.widgets.text("username", username)
spark.sql(f"CREATE DATABASE IF NOT EXISTS dbacademy_{username}")
spark.sql(f"USE dbacademy_{username}")
dbfs_path = f"/dbacademy/{username}/datasciencerapidstart/"
