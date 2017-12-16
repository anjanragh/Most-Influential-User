import MySQLdb
import plotly.plotly as py
import plotly.tools as tls
import plotly.graph_objs as go


db = MySQLdb.connect("localhost", "root", "123xyz", "twitterDB")
cursor = db.cursor()

x1 = []
y1 = []
y2 = []

sql = "select distinct scr_name, foll_count, foll_inf from htag2;"
cursor.execute(sql)
rows = cursor.fetchall()
for row in rows:
    x1.append(row[0])
    y1.append(row[1])
    y2.append(row[2])

data1 = [
    go.Bar(
        x=x1,
        y=y1,
        marker=dict(
            color='red'
            ),
        opacity=0.6
    )
]
data2 = [
    go.Bar(
        x=x1,
        y=y2,
        marker=dict(
            color='blue'
            ),
        opacity=0.6
    )
]

layout1 = go.Layout(
    title='Mapping Followers Count For Hashtag #TipuJayanti',
    xaxis=dict(
        title='User Screen Name'
    ),
    yaxis=dict(
        title='Follower Count of User'
    )
)

layout2 = go.Layout(
    title='Mapping Followers Influence For Hashtag #TipuJayanti',
    xaxis=dict(
        title='User Screen Name'
    ),
    yaxis=dict(
        title='Follower Influence of User'
    )
)

fig1 = go.Figure(data=data1, layout=layout1)
fig2 = go.Figure(data=data2, layout=layout2)

py.plot(fig1, filename='htag2_foll_count', validate=False)
py.plot(fig2, filename='htag2_foll_inf', validate=False)

del x1[:]
del y1[:]
del y2[:]

db.close()