
import mysql.connector
import json
from datetime import date, timedelta

# Connect with the MySQL Server
cnx = mysql.connector.connect(user='root', database='twitter', password='qaz12wsx')
cur = cnx.cursor(buffered=True)

week = '16,2017-10-09,2017-10-12' # 手动输入待获取数据的"week"字段的值

d_1 = week.split(',')[1].split('-')
d_2  = week.split(',')[2].split('-')
d1 = date(int(d_1[0]), int(d_1[1]), int(d_1[2]))  # start date
d2 = date(int(d_2[0]), int(d_2[1]), int(d_2[2]))  # end date

delta = d2 - d1         # timedelta
dates = []
for i in range(delta.days + 1):
    dates.append(str(d1 + timedelta(days=i)))

print('len of dates',len(dates))

sql = "SELECT fromUserScreenName, coordinates, date, fromUserId FROM twitter_all_add_date where week='{}' ORDER BY date".format(week) # sql语句

 #创建字典 http://www.runoob.com/python/python-dictionary.html 注意 字典是可以嵌套的 也就是 一个字典里 其子value可以是字典
cur.execute(sql) # 执行查询

mode = 'week' # 控制是按周来跑json还是按日期来跑json


loc_w = dict() # {index:[lat, lng, weight]}

if mode=='day':
    pass
    # dates = [] # 创建一个数组 用于存放该week的所有已扫描到的日期 (记录下这一周的所有日期 因为计算机要直接识别2017-07-31和2017-08-06的数字很麻烦 还不如自己循环一遍)
    # for line in cur: # cur存放了查询返回的所有数据 直接for循环 line代表一条数据记录  [fromUserName, coordinates, date]
    #     latlng = line[1][1:][:-1].split('/') # line[1] 代表 coordinates(e.g., [55.95/-3.18] )，使用[1:]去掉'['， [:-1]去掉']'  使用split分割经度和纬度
    #     date = str(line[2]) # line[2]代表日期 因为原本从数据库拿出来的数据是专用的日期格式的 所以我们需要将它转为str
    #     if date not in dates: # dates数组用来存放所有已扫描到的日期 如果date不在dates 代表是第一次遇见这个日期的数据
    #         dates.append(date) # 对于第一次遇见的新日期 插入dates数据 记录下来
    #         d[date] = dict() # 同时 在总词典 d中 为日期date 创建一个字典 用于存放该日期下的所有数据
    #     else:pass # 如果不是第一次遇见 忽略
    #
    #     if '/' in line[1]: # 如果/在line[1]里面 代表这条数据有经纬度 反之，代表这个数据没有经纬度 舍弃(pass)掉不要
    #         # d[date] 是我们刚才之前第一次遇见date时候创建的字典 用于存放数组的
    #         # len(d[date]) 返回d[date]字典的长度 这么做是因为 我们字典的的key是用0，1，2，3，4，...表示的
    #         # 当len = 0 也就是数组为空的时候， 我们在0的位置上，插入第一个数据，于是d[date][0] = [我们的数据(参考所需json格式)]。 以此类推，每个新插入的数据 其序号都是顺着之前的排下来的
    #         # 我们需要的json的格式是：{"0": [-3.46, 56.26, "用户名xxx"],...} 所以 经纬度(本身是str) 要转为float数字;line[0]即用户名本身是byte，使用decode转为str; date 之前已经转为str了 不用变。
    #         d[date][len(d[date])] = [float(latlng[1]), float(latlng[0]), line[0].decode("utf-8")]
    #     else:
    #         #反之，代表这个数据没有经纬度 舍弃掉不要
    #         pass
    # for date in d.keys(): # keys是获取大字典d的所有keys， in this case， 就是所有日期，每个日期及其下的数据字典用for循环loop一次
    #     with open('week_data/week'+week.split(',')[0]+'/week'+week.split(',')[0]+'-'+date+'.json','w+') as w: # 创建一个每个日期的json文件，设置为"w+"(代表可以写入) 称为w (变量名)
    #         w.write(json.dumps(d[date])) # json.dumps(字典) 可以直接返回规范的json格式。然后用w.write()直接讲返回的写入文件
    #
elif mode=='week':
    with open('week_data/week'+week.split(',')[0]+'/week'+week.split(',')[0]+'.json','w+') as w:
        d = dict() # 第一部分，记录{index: [lat, lng, sum_weight]}
        d_loc_count = dict()
        loc_u_d = dict() # 第二部分，记录{location: {userid:[userScreenName, weight]}}
        usr_sum_d = dict() # 第三部分，记录{userid:{date:weight,...,userName:userName}}
        for index, line in enumerate(cur): # cur存放了查询返回的所有数据 直接for循环 line代表一条数据记录  [fromUserName, coordinates, date]
            # print(index, line)
            userScreenName = line[0]
            latlng = line[1][1:][:-1]# line[1] 代表 coordinates(e.g., [55.95/-3.18] )，使用[1:]去掉'['， [:-1]去掉']'  使用split分割经度和纬度
            date = str(line[2])
            userId = line[3]
            # make d
            if latlng in d_loc_count.keys():
                d_loc_count[latlng] = d_loc_count[latlng] + 1
            else:
                d_loc_count[latlng] = 1

            # make loc_u_d
            if latlng in loc_u_d.keys():
                if userId in loc_u_d[latlng].keys():
                    loc_u_d[latlng][userId][1] = loc_u_d[latlng][userId][1] + 1
                else:
                    loc_u_d[latlng][userId] = [userScreenName,1]
            else:
                loc_u_d[latlng]=dict()
                loc_u_d[latlng][userId] = [userScreenName,1]

            # make usr_sum_d
            if userId in usr_sum_d.keys():
                usr_sum_d[userId][date] = usr_sum_d[userId][date] +1
                if latlng not in usr_sum_d[userId]['alllatlng']:
                    usr_sum_d[userId]['alllatlng'].append(latlng)
                else:
                    latlnglen = len(usr_sum_d[userId]['alllatlng'])
                    if usr_sum_d[userId]['alllatlng'][latlnglen-1] !=latlng:
                        usr_sum_d[userId]['alllatlng'].append(latlng)
                    else:pass

            else:
                usr_sum_d[userId] = dict()
                usr_sum_d[userId]['userName'] = userScreenName
                usr_sum_d[userId]['alllatlng'] = []
                for day in dates: # 所有预设为0
                    usr_sum_d[userId][day] = 0
                usr_sum_d[userId][date] = 1 # 当前日期额设置为1
                usr_sum_d[userId]['alllatlng'].append(latlng)


        print(d_loc_count)
        print(loc_u_d)
        print(usr_sum_d)


        # # write summary_location_dictionary:d_loc
        for index, key in enumerate(d_loc_count.keys()):
            if '/' in key:
                d[index] = [float(key.split('/')[1]), float(key.split('/')[0]), d_loc_count[key]]
            else:pass
        w.write(json.dumps(d))

        # write summary_location_user_dictionary: loc_u_d
        with open('week_data/week'+week.split(',')[0]+'/week'+week.split(',')[0]+'-location-user.json','w+') as w_1:
            w_1.write(json.dumps(loc_u_d))

        # write user_day_summary_dictionary: usr_sum_d
        with open('week_data/week'+week.split(',')[0]+'/week'+week.split(',')[0]+'-user-daysummary.json','w+') as w_1:
            w_1.write(json.dumps(usr_sum_d))


else:pass

cnx.close()
