import pymysql
import datetime
import YouTubeAPIDefine
import json

def ConnectDB(DBName):
    with open('DBConfig.json', 'r') as config:
        jsonConfig = json.load(config)

        return pymysql.connect(jsonConfig.host, jsonConfig.account, jsonConfig.password, DBName, cursorclass=pymysql.cursors.DictCursor)

def StoreToDB(APIData, DBName):
    if 0 != len(APIData):
        DB = ConnectDB(DBName)
        TableNameList = list(APIData.keys())

        CheckTableToCreate(DB, DBName, TableNameList)

        for APIMethod,APIDataList in APIData.items():
            if 0 != len(APIDataList):
                TableName = APIMethod
                Cursor = DB.cursor()
                CheckColToADD(DB, APIDataList, DBName, TableName)
                try:
                    for UserAPIData in APIDataList:
                        InsetPlaceHolders = ', '.join(['%s'] * len(UserAPIData))  # Create %s for insertion
                        InsetColumns = ', '.join(UserAPIData.keys())  # Col to be inseted
                        SqlInsetToDB = "INSERT INTO %s (%s) VALUES (%s)" % (TableName, InsetColumns, InsetPlaceHolders)

                        Cursor.execute(SqlInsetToDB, list(UserAPIData.values()))
                except:
                    DB.rollback()
                    print(("StoreToDB: Sql Execution Failed, insert into table %s Failed.") % TableName)
                    return -1
                else:
                    DB.commit()
                    print(("StoreToDB: Sql Execution Successed, insert into table %s Successfully.") % TableName)
        DB.close()
    return 0

def GetFromDB(DBName, SelectedMethodNumList, SelectAll=False, StartTimeList=[0,0,30], EndTimeList=[0,0,0]): #days hours mins
    DB = ConnectDB(DBName)
    TableNameList = YouTubeAPIDefine.GetAPIMethod(SelectedMethodNumList)
    ResultDict = {}

    for TableName in TableNameList:
        if (True == SelectAll) :
            SqlSelect = "SELECT * FROM %s ORDER BY datetime;" % TableName  # select all order by datetime
        else :
            # Adjust the start time of the data to select
            TimeStart = (datetime.datetime.now() - datetime.timedelta(days=StartTimeList[0],hours=StartTimeList[1],minutes=StartTimeList[2])).replace(microsecond=0).isoformat(' ')
            # Adjust the end time of the data to select
            TimeEnd = (datetime.datetime.now() - datetime.timedelta(days=EndTimeList[0], hours=EndTimeList[1], minutes=EndTimeList[2])).replace(microsecond=0).isoformat(' ')
            # Prevent Invalid Input
            if TimeStart > TimeEnd:
                TimeEnd = datetime.datetime.now()
            SqlSelect = "SELECT * FROM %s WHERE datetime BETWEEN '%s' AND '%s'" % (TableName,TimeStart,TimeEnd)

        Cursor = DB.cursor()

        try:
            Cursor.execute(SqlSelect)
            SqlResult = Cursor.fetchall() #SqlResult is already a dictionary
            ResultDict[TableName] = PutResultIntoDictById(SqlResult)
        except:
            print(("GetFromDB: Sql Execution Failed, get data from table %s Failed.") % TableName)
            return -1
        else:
            print(("GetFromDB: Sql Execution Successed, get data from table %s Successfully.") % TableName)

    DB.close()

    return ResultDict

def CheckTableToCreate(DB, DBName, TableNameList):
    Cursor = DB.cursor()
    SqlGetTable = "SELECT table_name FROM INFORMATION_SCHEMA.tables " + \
                  "WHERE TABLE_SCHEMA='" + DBName + "';"
    SqlCreateTable = "CREATE TABLE {} (" + \
                     "id varchar(32), " + \
                     "dateTime DATETIME)"

    Cursor.execute(SqlGetTable)
    SQLResult = Cursor.fetchall()
    CurrentTableList = PutResultIntoList(SQLResult)
    TableToCreate = list(set(TableNameList) - set(CurrentTableList))
    if 0 != len(TableToCreate):
        try:
            for Table in TableToCreate:
                Cursor.execute(SqlCreateTable.format(Table))
        except:
            DB.rollback()
            print(("CheckTableToCreate: Sql Execution Failed, add new table %s Failed.") % Table)
        else:
            DB.commit()
            print(("CheckTableToCreate: Sql Execution Successed, add new table %s Successfully.") % ', '.join(TableToCreate))
    else:
        print("CheckTableToCreate: No Table need to be added.")

def CheckColToADD(DB, APIDataList, DBName, TableName):
    PrevColList = []
    ColToAdd = []

    for APIData in APIDataList:
        if 0 == len(PrevColList):
            Cursor = DB.cursor()

            SqlGetCol = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS " + \
                        "WHERE TABLE_SCHEMA='" + DBName + "' AND TABLE_NAME='" + TableName + "';"

            Cursor.execute(SqlGetCol)
            SQLResult = Cursor.fetchall()
            CurrentColList = PutResultIntoList(SQLResult)
        else:
            CurrentColList = PrevColList

        ColToAdd += (list(set(APIData.keys()) - set(CurrentColList)))
        PrevColList = CurrentColList + ColToAdd

    if 0 != len(ColToAdd):
        try:
            SqlAddCol = "ALTER TABLE `" + DBName + "`.`" + TableName + "` ADD COLUMN `{0}` varchar(32);";
            for Col in ColToAdd:
                Cursor.execute(SqlAddCol.format(Col))

        except:
            DB.rollback()
            print(("CheckColToADD: Sql Execution Failed, add Col %s Failed.") % Col)
        else:
            DB.commit()
            print(("CheckColToADD: Sql Execution Successed, add Col %s Successfully.") % ', '.join(ColToAdd))

    else:
        print("CheckColToADD: No Col need to be added.")

def PutResultIntoList(SQLResult):
    ResultList = []
    for Row in SQLResult:
        ResultList.append('%s' % Row)
    return ResultList

def PutResultIntoDictById(SQLResult):
    ResultDict = {}
    for Row in SQLResult:
        if Row['id'] not in ResultDict:
            ResultDict[Row['id']] = []

        ResultDict[Row['id']].append(Row)

    return ResultDict
