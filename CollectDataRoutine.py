#! python3

import GetFromAPI
import MyYouTubeDB
import threading
import datetime

def MainRoutine():
    # Run the process every N seconds
    Timer = 60.0
    threading.Timer(Timer, MainRoutine).start()  # called every minute

    Key = 'YOUTUBE-API-KEY'
    SelectedMethodNumList = [1, 6]
    SelectedUserNameList = ["Pewdiepie", "LifeAccordingToJimmy", "jasonjason1124"]

    YouTubeData = GetFromAPI.GetYouTubeData(SelectedMethodNumList, SelectedUserNameList, Key)
    if (-1 != MyYouTubeDB.StoreToDB(YouTubeData, 'StorageDB')):
        print(("%s : Data collection process Successed") % datetime.datetime.now().replace(microsecond=0).isoformat(' '))
    else:
        print(("%s : Data collection process Failed") % datetime.datetime.now().replace(microsecond=0).isoformat(' '))

def main():
    MainRoutine()

if __name__ == "__main__":
    main()