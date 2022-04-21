import boto3
import pandas as pd
client=boto3.client("s3",region_name='me-south-1')
ids=[]
fristTimeIdCheck=True

def proccommand(command):
    
    
    txt=command.replace("@nfl_stats_Ar", "")
    txt=txt.replace("\n", "")
    print(txt)
    check="احسب"
    if(check in txt):
        txt=txt.replace(check, "")
    


                
            
        keyWord=["ضد","vs","VS","Vs","مقابل","امام","أمام"]
        for key in keyWord:
            
           
            if(key in txt):
                    
                t1=txt[0:txt.index(key)-1]
                t1NoSp=t1.replace(" ", "")
                
                t2=txt[txt.index(key)+len(key):len(txt)] 
                t2NoSp=t2.replace(" ", "")
                
                team1=Teamabbreviation(t1NoSp)
                if(t1[0]==" "):
                    t1=t1[1:]
                    if(t1[0]==" "):
                        t1=t1[1:]
                
                team2=Teamabbreviation(t2NoSp)
                if(team1==""or team2==""):
                    return "NULL"
                else:
                    return(teamVsteam(team1,t1,team2,t2))
        t1=txt.replace(" ", "")
        if(Teamabbreviation(t1)==""):
            return "NULL"
           
        return(oneteam(Teamabbreviation(t1),txt))
    return "skip"
def Teamabbreviation(name):
    abbreviation=""
    findabbreviation = client.select_object_content(
        Bucket="nflstats",
        Key="teams.csv",
        ExpressionType='SQL',
        Expression="SELECT s.abbreviation FROM s3object s where s.name='"+name+"'",
        InputSerialization = {'CSV': {"FileHeaderInfo": "Use"}},
        OutputSerialization = {'CSV': {}},
        )
    for i in findabbreviation["Payload"]:
        
        if "Records" in i:
            abbreviation=(i["Records"]["Payload"].decode())
    if(len(abbreviation)==0):
        return ""
    return (abbreviation.split()[0])



def oneteam(team,teamName):
    t1="لعب"
    t2="مباراة"
    t3="و انتصر في"
    t4="وخسر"
    t5="منذ تأسيس الدوري"
    t6="وتعادل"
    win=0
    total=0
    all=0
    lose=0
    draw=0
    totals = client.select_object_content(
        Bucket="nflstats",
        Key="1970to2021.csv",
        ExpressionType='SQL',
        Expression="SELECT * FROM s3object s where s.Team1='"+team+"' OR s.Team2='"+team+"' " ,
        InputSerialization = {'CSV': {"FileHeaderInfo": "Use"}},
        OutputSerialization = {'CSV': {}},
        )

    for i in totals["Payload"]:
        if "Records" in i:
            all=(i["Records"]["Payload"].decode())
    
    all=list(all.split("\r\n"))
    for i in all:
        all[(all.index(i))]=i.split(",")
    del all[-1]
    for match in all:
        total=total+1

        if(match[5]==match[6]):
            draw=draw+1
        elif((match[3]==team and match[5]>match[6])or(match[4]==team and match[6]>match[5])):
            win=win+1
        else:
            lose=lose+1
    
    return t1+" "+teamName+" "+str(total)+" "+t2+"  "+t3+" "+str(win)+" "+t4+" "+str(lose)+" "+t6+" "+str(draw)+" "+t5


def teamVsteam(team1,team1Name,team2,team2Name):
    txt=[" مباريات لعبت بين"," و انتصر"," منذ تأسيس الدوري","وخسر",'وتعادل']
    win=0
    total=0
    all=0
    lose=0
    draw=0
    totals = client.select_object_content(
        Bucket="nflstats",
        Key="1970to2021.csv",
        ExpressionType='SQL',
        Expression="SELECT * FROM s3object s where s.Team1='"+team1+"' and s.Team2='"+team2+"'  OR s.Team1='"+team2+"' and s.Team2='"+team1+"' " ,
        InputSerialization = {'CSV': {"FileHeaderInfo": "Use"}},
        OutputSerialization = {'CSV': {}},
        )
    for i in totals["Payload"]:
        if "Records" in i:
            all=(i["Records"]["Payload"].decode())
    
    all=list(all.split("\r\n"))
    for i in all:
        all[(all.index(i))]=i.split(",")
    del all[-1]
    for match in all:
        total=total+1

        if(match[5]==match[6]):
            draw=draw+1
        elif((match[3]==team1 and match[5]>match[6])or(match[4]==team1 and match[6]>match[5])):
            win=win+1
        else:
            lose=lose+1
    return str(total)+" "+str(txt[0])+" "+team1Name+" و "+team2Name+" "+str(txt[1])+" "+team1Name+" في "+str(win)+""+str(txt[3])+" "+str(lose)+" "+str(txt[4])+" "+str(draw)+" "+str(txt[2])


def idDupChecker(ids,id):
    for i in ids:
        if(i==str(id)):
            return [True,ids]
    ids=addid(ids,id)
    updatacsv(id)
    return [False,ids]


def addid(idslist,id):
    
    idslist.append(str(id))
    
    return idslist


def updatacsv(id):
    print("update")
    initial_df = pd.read_csv('/tmp/id.csv')
    df = pd.DataFrame(initial_df)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    newData=pd.DataFrame({'id': [id],'a':"0"})
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    newData.to_csv('/tmp/id.csv', mode='a', index=False, header=False)
    

def uploadFile():
    with open("/tmp/id.csv", "rb") as f:
        client.upload_fileobj(f, "nflstats", "id.csv")

def getallid(fristTimeIdCheck):
    if(fristTimeIdCheck):
        print("here")
        findId = client.select_object_content(
        Bucket="nflstats",
        Key="id.csv",
        ExpressionType='SQL',
        Expression="SELECT s.id FROM s3object s",
        InputSerialization = {'CSV': {"FileHeaderInfo": "Use"}},
        OutputSerialization = {'CSV': {}},
        )
        for i in findId["Payload"]:
            if "Records" in i:
                ids=(i["Records"]["Payload"].decode())

    return ids.replace("\n", " ").split(" ")

