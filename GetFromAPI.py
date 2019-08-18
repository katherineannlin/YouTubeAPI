import urllib.request
import ssl
import json
import datetime
import YouTubeAPIDefine
import codecs

def GetYouTubeData(SelectedMethodNumList, SelectedUserNameList, Key):
    APIMethod = YouTubeAPIDefine.GetAPIMethod(SelectedMethodNumList)
    APIUserId = GetUserIdByName(SelectedUserNameList, Key)
    APIResponse = {}

    for Method in APIMethod:
        APIUrl = 'https://www.googleapis.com/youtube/v3/channels?'
        APIParams = {}
        APIParams["part"] = Method
        APIParams["id"] = APIUserId
        APIParams["key"] = Key

        APIResponse[Method] = SendAPIRequest(APIUrl, APIParams)
    YoutubeData = ParseAPIResponse(APIResponse)
    return YoutubeData

def SendAPIRequest(APIUrl, APIParams):
    APIParams = "&".join(['%s=%s' % (key, value) for (key, value) in APIParams.items()]) #to string
    APIRequest = APIUrl + APIParams
    Context = ssl._create_unverified_context() #Need to send request
    Reader = codecs.getreader("utf-8")
    return json.load(Reader(urllib.request.urlopen(APIRequest, context=Context)))

def ParseAPIResponse(APIResponse):
    ParseResult = {}

    for MethodKey, MethodValue in APIResponse.items():
        ParseResult[MethodKey] = []
        for User in MethodValue['items']:
            UserDict = {}
            UserDict['id'] = User['id']
            UserDict['dateTime'] = datetime.datetime.now().replace(microsecond=0).isoformat(' ')
            for MethodItemsKey, MethodItemsValue in User[MethodKey].items():
                if isinstance(MethodItemsValue, dict):
                    for MethodDetailKey, MethodDetailValue in MethodItemsValue.items():
                        UserDict[MethodDetailKey] = MethodDetailValue
                else:
                    UserDict[MethodItemsKey] = MethodItemsValue
            ParseResult[MethodKey].append(UserDict)

    return ParseResult

def GetUserIdByName(SelectedUserNameList, Key):
    UserIdList = []
    NewUserList = []
    UserConfig = {}
    blUserConfigChanged = False

    try:
        with open('UserNameIdMap.json', 'r') as UserConfigFile:
            UserConfig = json.load(UserConfigFile)
            for UserName in SelectedUserNameList:
                if UserName in UserConfig: #userconfig = saved username
                    UserIdList.append(UserConfig[UserName]) # return string of useridlist
                else :
                    NewUserList.append(UserName)
    except IOError:
        NewUserList = SelectedUserNameList

    for UserName in NewUserList:
        APIUrl = 'https://www.googleapis.com/youtube/v3/channels?'
        APIParams = {}
        APIParams["part"] = "id"
        APIParams["forUsername"] = UserName
        APIParams["key"] = Key

        APIResponse = SendAPIRequest(APIUrl, APIParams)
        UserIdList.append(APIResponse["items"][0]['id'])
        UserConfig[UserName] = APIResponse["items"][0]['id'] #saving channel ID as the value using username as key
        blUserConfigChanged = True #adding new user will change user config

    if True == blUserConfigChanged:
        with open('UserNameIdMap.json', 'w') as UserConfigFile:
            json.dump(UserConfig, UserConfigFile)

    return ",".join(UserIdList)
