
### connecting to azure cloud SQL database

connection = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:sushmak.database.windows.net,1433;Database=quakes;Uid=sushma@sushmak;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
cursor = connection.cursor()



--------------------------------------------------------------------------------
@app.route('/')
def index():
    start_time = time()
    cursor.execute("CREATE TABLE [dbo].[codes](\
        [code] [nvarchar](8) NULL,\
        [state] [nvarchar](20) NULL)")
    connection.commit()

    query = "INSERT INTO dbo.codes(code,state) VALUES (?,?)"
    

    with open('statecode.csv') as csvfile:
        next(csvfile)
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            print(row)
            cursor.execute(query,row)
        connection.commit()
        end_time = time()
        time_taken = (end_time - start_time)
        flash('The Average Time taken to import to cloud database from csv is : ' + "%.4f" % time_taken + " seconds")
        cursor.close()
    return render_template('index.html', t=time_taken)
-------------------------------------------------------------------------------



@app.route('/')
def index():
    start_time = time()
    cursor.execute("CREATE TABLE [dbo].[popul](\
        [state] [nvarchar](20) NULL,\
	[2010] [nvarchar](10) NULL,\
	[2011] [nvarchar](10) NULL,\
	[2012] [nvarchar](10) NULL,\
	[2013] [nvarchar](10) NULL,\
	[2014] [nvarchar](10) NULL,\
	[2015] [nvarchar](10) NULL,\
	[2016] [nvarchar](10) NULL,\
	[2017] [nvarchar](10) NULL,\
        [2018] [nvarchar](10) NULL)")
    connection.commit()

    query = "INSERT INTO dbo.popul(state,2010,2011,2012,2013,2014,2015,2016,2017,2018) VALUES (?,?)"
    

    with open('population.csv') as csvfile:
        next(csvfile)
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            print(row)
            cursor.execute(query,row)
        connection.commit()
        end_time = time()
        time_taken = (end_time - start_time)
        flash('The Average Time taken to import to cloud database from csv is : ' + "%.4f" % time_taken + " seconds")
        cursor.close()
    return render_template('index.html', t=time_taken)
---------------------------------------------------------------------------------


@app.route('/')
def index():
    start_time = time()
    cursor.execute("CREATE TABLE [dbo].[county](\
        [county] [nvarchar](8) NULL,\
        [state] [nvarchar](20) NULL)")
    connection.commit()

    query = "INSERT INTO dbo.county(county,state) VALUES (?,?)"
    

    with open('counties.csv') as csvfile:
        next(csvfile)
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            print(row)
            cursor.execute(query,row)
        connection.commit()
        end_time = time()
        time_taken = (end_time - start_time)
        flash('The Average Time taken to import to cloud database from csv is : ' + "%.4f" % time_taken + " seconds")
        cursor.close()
    return render_template('index.html', t=time_taken)






 7. Allow a user, through a web form, to give a two letter state name (for example "TX" for Texas), and a year, and you 
    show the state population for that year.
@app.route('/server', methods=['GET', 'POST'])
def server():
    tag1 = request.form['tag1']
    s = time()
    year = request.form['tag2'] 

    if request.method == 'POST':
        query = "SELECT "+ year +" FROM dbo.popul where state = (Select state from dbo.codes where code ="+ "'"+tag1+"')"
        print(query)
        cursor.execute(query)
        r = cursor.fetchall()
        print(r)
    e = time()
    t = e-s
    return render_template('display.html', t=str(t), re=r)



 8. Allow a user, through a web form, to give a two letter state name, for all counties in that state, you show count 
    (total, number of counties) followed by a list of all counties in that state.

@app.route('/qstn8', methods=['GET', 'POST'])
def qstn8():
    tag1 = request.form['tag8']
    query = "select county from dbo.county where state =(Select state from codes where code ="+"'"+tag1+"')"
    if request.method == 'POST':
        print(query)
        s = time()
        cursor.execute(query)
        r = cursor.fetchall()
        print(r)
    e = time()
    t = e-s
    return render_template('display.html', data=r, t=t)


 9. Allow a user, through a web form, to give a year and population range, and you show the names of the states within that
    population range.

@app.route('/qstn9', methods=['GET', 'POST'])
def qstn9():
    tag1 = request.form['tag1']
    tag2 = request.form['tag2']
    year = request.form['y']
    s = time()

    query = "select state from popul where "+year+" BETWEEN " +"'"+tag1+"'"+ " AND " + "'"+tag2+"'"
    if request.method == 'POST':
        print(query)
        cursor.execute(query)
        r = cursor.fetchall()
        print(r)
    e = time()
    t = e-s
    return render_template('display.html', data1=r, t1=t)

 10. A user will give you the number of times to do the "query", you will do the previous steps 8. and 9. that number of times 
     and show the results as well as the total time taken.
@app.route('/qstn10', methods=['GET', 'POST'])
def qstn10():
    tag1 = request.form['tag10']
    s = time()
    for i in range(tag1):
        qstn8()
        qstn9()
    e = time()
    t = e-s
    return render_template('magGreater.html', t=t)

 

 11. Repeat 10. using an in-memory caching mechanism (NOT an in-memory database) repeat the previous step, then show us.
r = redis.StrictRedis(host='sushma.redis.cache.windows.net', port=6380, db=0, password='fQrhWzt3pQ5QnCBWDzM6GhSQCBCi8p33qLGVexTPn8I=', ssl=True)

@app.route('/serverCache', methods=['GET', 'POST'])
def serverCache():
    user_number_of_time = request.form['tag1']
 	for i in range (user_number_of_time):    
		#qstn8
		queryString1 = "select county from dbo.county where state =(Select state from codes where code ="+"'"+tag1+"')"
		#qstn9
		queryString2 = "select state from popul where "+year+" BETWEEN " +"'"+tag1+"'"+ " AND " + "'"+tag2+"'"

    if r.get(queryString) == None:
        s = time()
        cursor.execute(queryString1)
	cursor.execute(queryString2)
        data = cursor.fetchall()
    else:
        s = time()
        data = r.get(queryString1)
	data = r.get(queryString1)
    e = time()
    t = e - s
    return render_template('display.html', t1=t, re1=data)


 12. Show GTA parts (6 not necessary), 7, 8, 9, 10, 11
     (as soon as you complete any part.)

 13. When complete, return (send) this quiz
     If you finish early, send this immediately, otherwise send between
     the end of class and no more than 1 minute after that.

 Good Luck!











 

