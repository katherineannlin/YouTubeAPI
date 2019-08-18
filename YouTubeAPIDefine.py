def GetAPIMethod(SelectedMethodNumList):
    DefinePartsDict = {
        0: 'snippet',
        1: 'contentDetails',
        2: 'fileDetails',
        3: 'player',
        4: 'processingDetails',
        5: 'recordingDetails',
        6: 'statistics',
        7: 'status',
        8: 'suggestions',
        9: 'topicDetails'
    }
    SelectedMethodList = []
    for num in SelectedMethodNumList:
        if num in DefinePartsDict:
            SelectedMethodList.append(DefinePartsDict[num])

    return SelectedMethodList
