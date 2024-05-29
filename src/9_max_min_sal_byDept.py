# Databricks notebook source
data = [('Genece' , 2 , 75000),
('𝗝𝗮𝗶𝗺𝗶𝗻' , 2 , 80000 ),
('𝗣𝗮𝗻𝗸𝗮𝗷' , 2 , 80000 ),
('Tarvares' , 2 , 70000),
('Marlania' , 4 , 70000),
('Briana' , 4 , 85000),
('𝗞𝗶𝗺𝗯𝗲𝗿𝗹𝗶' , 4 , 55000),
('𝗚𝗮𝗯𝗿𝗶𝗲𝗹𝗹𝗮' , 4 , 55000),  
('Lakken', 5, 60000),
('Latoynia' , 5 , 65000) ]

schema = "emp_name string, dept_id int, salary int"
df = spark.createDataFrame(data,schema)
df.createOrReplaceTempView('df')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select dept_id, max(salary) as max_sal
# MAGIC from df
# MAGIC group by dept_id;
# MAGIC
# MAGIC with df2 as (
# MAGIC select dept_id, emp_name, salary,
# MAGIC   dense_rank() over (partition by dept_id 
# MAGIC                       order by salary desc) as max_sal_rank
# MAGIC from df)
# MAGIC select * from df2
# MAGIC where max_sal_rank = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC with df2 as (
# MAGIC select dept_id, emp_name, salary,
# MAGIC   dense_rank() over (partition by dept_id 
# MAGIC                       order by salary desc) as max_sal_rank
# MAGIC from df)
# MAGIC select dept_id, salary, collect_list(emp_name) as emp_names
# MAGIC from df2
# MAGIC where max_sal_rank = 1
# MAGIC group by dept_id, salary;

# COMMAND ----------

