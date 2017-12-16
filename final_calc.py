import MySQLdb

db = MySQLdb.connect("localhost", "root", "123xyz", "twitterDB")
cursor = db.cursor()

sql = "create table htag5_final as select scr_name, foll_inf, avg(ret_inf) as avg_ret_inf, avg(fav_inf) as avg_fav_inf from htag5 group by scr_name;"
cursor.execute(sql)
db.commit()

sql = "select scr_name from ((select scr_name, foll_inf as 'INF VALUE' from htag5_final order by foll_inf desc limit 8) UNION \
(select scr_name, avg_ret_inf from htag5_final order by avg_ret_inf desc limit 8) UNION \
(select scr_name, avg_fav_inf from htag5_final order by avg_fav_inf desc limit 8)) t \
group by scr_name having count(*)=3;"
cursor.execute(sql)
rows = cursor.fetchall()
if not rows:
    print "No highly influential users."
else:
    print "Highly Influential Users are: "
    for row in rows:
        print row[0]

sql = "select scr_name from ((select scr_name, foll_inf as 'INF VALUE' from htag5_final order by foll_inf desc limit 8) UNION \
(select scr_name, avg_ret_inf from htag5_final order by avg_ret_inf desc limit 8) UNION \
(select scr_name, avg_fav_inf from htag5_final order by avg_fav_inf desc limit 8)) t \
group by scr_name having count(*)=2;"
cursor.execute(sql)
rows = cursor.fetchall()
if not rows:
    print "No influential users."
else:
    print "Influential Users are: "
    for row in rows:
        print row[0]

sql = "select scr_name from ((select scr_name, foll_inf as 'INF VALUE' from htag5_final order by foll_inf desc limit 8) UNION \
(select scr_name, avg_ret_inf from htag5_final order by avg_ret_inf desc limit 8) UNION \
(select scr_name, avg_fav_inf from htag5_final order by avg_fav_inf desc limit 8)) t \
group by scr_name having count(*)=1;"
cursor.execute(sql)
rows = cursor.fetchall()
if not rows:
    print "No potential influential users."
else:
    print "Potential Influential Users are: "
    for row in rows:
        print row[0]

sql="drop table htag5_final;"
cursor.execute(sql)
db.commit()

db.close()