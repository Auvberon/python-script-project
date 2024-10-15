import os
import glob
import shutil
from datetime import datetime
from pathlib import Path

appName = [
    ["/mnt/nfsshare/Nexauth/logs", "nexauth"],
    ["/mnt/nfsshare/nexconsent/logs", "nexconsent"],
    ["/mnt/nfsshare/Farmindo/logs", "farmindo"],
    ["/mnt/nfsshare/Farmindo/logs", "farmindopo"],
    ["/mnt/nfsshare/nexcare/logs", "nexcare"],
    ["/mnt/nfsshare/nextrac/logs", "nextrac"]
    ]

now = datetime.now()
day = now.strftime("%d")
month = now.strftime("%m")
year = now.strftime("%y")

for i in range(len(appName)):
    os.chdir(appName[i][0])
    print(os.getcwd())

    if os.path.isdir(month) == False:
        os.mkdir(appName[i][0] + "/" + month)
    else:
        print("Directory Exists")

    # os.rename(appName[i][0] + "/" + appName[i][1] + ".log", appName[i][0] + "/" + month + "/" + appName[i][1] + day + month + ".log")
    # Path(appName[i][0] + "/" + appName[i][1] + ".log").touch()

    shutil.copyfile(appName[i][0] + "/" + appName[i][1] + ".log", appName[i][0] + "/" + month + "/" + appName[i][1] + day + month + ".log")
    f = open(appName[i][0] + "/" + appName[i][1] + ".log", 'r+')
    f.truncate(0)

    threeMonth = int(month) - 3
    if threeMonth == -2:
        files = glob.glob(appName[i][0] + "/10/*.log")
        for f in files:
            os.remove(f)
    elif threeMonth == -1:
        files = glob.glob(appName[i][0] + "/11/*.log")
        for f in files:
            os.remove(f)
    elif threeMonth == 0:
        files = glob.glob(appName[i][0] + "/12/*.log")
        for f in files:
            os.remove(f)
    else:
        files = glob.glob(appName[i][0] + "/" + str(threeMonth) + "/*.log")
        for f in files:
            os.remove(f)


