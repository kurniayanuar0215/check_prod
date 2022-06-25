import imgkit
import mysql.connector
import pandas as pd
import os

# IMGKIT CONFIG
options = {
    'format': 'jpg',
    'crop-w': '987',
    'encoding': "UTF-8",
    'enable-local-file-access': None
}

# DB CONFIG
connection = mysql.connector.connect(
    host="10.47.150.144",
    user="sqaviewjbr",
    password="5qAv13wJbr@2019#",
    database="kpi"
)

try:
    siteid = param[1]
    query = '''SELECT tanggal,
    ROUND(SUM(`traffic_2g_total_erlang`),2) traffic_2g,
    ROUND(SUM(`traffic_3g_total_erlang`),2) traffic_3g,
    ROUND(SUM(`payload_2g_total_mbit`/8192),2) payload_2g_GB,
    ROUND(SUM(`payload_3g_total_mbit`/8192),2) payload_3g_GB,
    ROUND(SUM(`payload_4g_total_mbit`/8192),2) payload_4g_GB,
    ROUND(SUM(`traffic_total_erlang`),2) total_traffic,
    ROUND(SUM(`payload_total_mbit`/8192),2) payload_total_GB
    FROM `productivity_daily_siteid`
    WHERE region = "JABAR" AND siteid = "'''+siteid+'''"
    GROUP BY tanggal
    ORDER BY tanggal DESC
    LIMIT 7;'''

    html_string = '''
    <html>
      <head><title>Productivity</title></head>
      <link rel="stylesheet" type="text/css" href="F:/KY/check_prod/df_style.css"/>
      <body>
        <center><h3>Lastest Productivity '''+siteid+''' DB 144</h3></center>
        {table_prod}
      </body>
    </html>
    '''

    my_data = pd.read_sql(query, connection)

    with open('F:/KY/check_prod/index.html', 'w') as f:
        f.write(html_string.format(
            table_prod=my_data.to_html(classes='mystyle', index=False)))

except IndexError:
    siteid = 'JABAR'
    query = '''SELECT tanggal,
    ROUND(SUM(`traffic_2g_total_erlang`),2) traffic_2g,
    ROUND(SUM(`traffic_3g_total_erlang`),2) traffic_3g,
    ROUND(SUM(`payload_2g_total_mbit`/8192),2) payload_2g_GB,
    ROUND(SUM(`payload_3g_total_mbit`/8192),2) payload_3g_GB,
    ROUND(SUM(`payload_4g_total_mbit`/8192),2) payload_4g_GB,
    ROUND(SUM(`traffic_total_erlang`),2) total_traffic,
    ROUND(SUM(`payload_total_mbit`/8192),2) payload_total_GB
    FROM `productivity_daily_siteid`
    WHERE region = "JABAR"
    GROUP BY tanggal
    ORDER BY tanggal DESC
    LIMIT 7;'''

    query_kpi = '''
    (SELECT 'kpi_daily_2g', `tanggal`,
    ROUND(SUM(tch_traffic_erl),2) traffic,
    ROUND(SUM(gprs_payload_mbit+edge_payload_ul_mbit+edge_payload_dl_mbit)/8192,2) payload_GB
    FROM `kpi`.`kpi_daily_2g`
    WHERE LEFT(siteid,3) IN (
        'BDG', 'BDK', 'BDS', 'CMI', 'COD', 'BDB',
        'IND', 'SUB', 'CRB', 'CMS', 'KNG', 'MJL',
        'CJR', 'SMD', 'BJR', 'TSK', 'GRT', 'PAN', 'BDX')
    GROUP BY `tanggal`
    ORDER BY `tanggal` DESC
    LIMIT 1)

    UNION

    (SELECT 'kpi_daily_3g', `tanggal`,
    ROUND(SUM(traffic_voice_erlang+traffic_video_erlang),2) traffic,
    ROUND(SUM(ps_payload_dl_mbit+ps_payload_ul_mbit+hsdpa_payload_mbit+hsupa_payload_mbit)/8192,2) payload_GB
    FROM `kpi`.`kpi_daily_3g`
    WHERE LEFT(siteid,3) IN (
        'BDG', 'BDK', 'BDS', 'CMI', 'COD', 'BDB',
        'IND', 'SUB', 'CRB', 'CMS', 'KNG', 'MJL',
        'CJR', 'SMD', 'BJR', 'TSK', 'GRT', 'PAN', 'BDX')
    GROUP BY `tanggal`
    ORDER BY `tanggal` DESC
    LIMIT 1)

    UNION

    (SELECT 'kpi_daily_4g', `tanggal`, 0,
    ROUND(SUM(`traffic_dl_volume_mbit`+`traffic_ul_volume_mbit`)/8192,2) payload_GB
    FROM `kpi`.`kpi_daily_4g`
    WHERE LEFT(siteid,3) IN (
        'BDG', 'BDK', 'BDS', 'CMI', 'COD', 'BDB',
        'IND', 'SUB', 'CRB', 'CMS', 'KNG', 'MJL',
        'CJR', 'SMD', 'BJR', 'TSK', 'GRT', 'PAN', 'BDX')
    GROUP BY `tanggal`
    ORDER BY `tanggal` DESC
    LIMIT 1);
    '''

    query_kpi_simple = '''SELECT 'kpi_daily_2g', MAX(`tanggal`) LAST_DAY FROM kpi.`kpi_daily_2g`
    UNION
    SELECT 'kpi_daily_3g', MAX(`tanggal`) LAST_DAY FROM kpi.`kpi_daily_3g`
    UNION
    SELECT 'kpi_daily_4g', MAX(`tanggal`) LAST_DAY FROM kpi.`kpi_daily_4g`;'''

    query_hourly = '''
    SELECT 'kpi_hourly_2g', LEFT(MAX(a.`jam`),10) last_day, RIGHT(MAX(a.`jam`),9) last_hour, COUNT(a.jam) count_hour, IF(COUNT(a.jam)-LEFT(RIGHT(MAX(a.`jam`),9),2)-1,'OK','NOK') status FROM
    (SELECT DISTINCT jam FROM `kpi_hourly_2g`) a
    WHERE LEFT(a.jam,10) = (SELECT LEFT(MAX(jam),10) last_date FROM kpi_hourly_2g)
    UNION
    SELECT 'kpi_hourly_3g', LEFT(MAX(a.`jam`),10) last_day, RIGHT(MAX(a.`jam`),9) last_hour, COUNT(a.jam) count_hour, IF(COUNT(a.jam)-LEFT(RIGHT(MAX(a.`jam`),9),2)-1,'OK','NOK') ststus FROM
    (SELECT DISTINCT jam FROM `kpi_hourly_3g`) a
    WHERE LEFT(a.jam,10) = (SELECT LEFT(MAX(jam),10) last_date FROM kpi_hourly_3g)
    '''

    html_string = '''
    <html>
      <head><title>Productivity</title></head>
      <link rel="stylesheet" type="text/css" href="F:/KY/check_prod/df_style.css"/>
      <body>
        <center><h3>Lastest Productivity '''+siteid+''' DB 144</h3></center>
        <h4>Table productivity daily siteid</h4>
        {table_prod}
        <h4>Table KPI daily</h4>
        {table_kpi}
      </body>
    </html>
    '''

    my_data = pd.read_sql(query, connection)
    my_data_kpi = pd.read_sql(query_kpi_simple, connection)
    my_data_kpi.rename(columns={'kpi_daily_2g': 'table'}, inplace=True)
    # my_data_kpi_hourly = pd.read_sql(query_hourly, connection)
    # my_data_kpi_hourly.rename(columns={'kpi_hourly_2g': 'table'}, inplace=True)

    with open('F:/KY/check_prod/index.html', 'w') as f:
        f.write(html_string.format(
            table_prod=my_data.to_html(classes='mystyle', index=False),
            table_kpi=my_data_kpi.to_html(classes='mystyle', index=False)))

imgkit.from_file('F:/KY/check_prod/index.html',
                 'F:/KY/check_prod/out.jpg', options=options)
