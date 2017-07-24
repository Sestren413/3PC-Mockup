__author__ = 'Jordan'

from multiprocessing import Process,Pipe
import socket
import select
import time
import pickle
import copy


def proc(num,numprocs,pipe,clean):

    # process init
    baseport = 56000

    if clean:
        name = "p"+num
        print(name,"created")
        myport = baseport + int(num)
        servsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servsoc.bind(('localhost',myport))
        servsoc.listen(int(numprocs))
        csocs = {}
        connecting = True
        while connecting:
            for i in range(int(numprocs)):
                port = baseport + i
                c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    if csocs[i] == 'error' or not csocs[i]:
                        c.connect(('localhost',port))
                        csocs[i] = c
                        c.send(int(num).to_bytes(1,'big'))
                except:
                    # print(name,"failed connection to ",port)
                    csocs[i] = 'error'
                # else:
                    # print('clean')
            connecting = False
            # print (name,csocs)
            for i in range(int(numprocs)):
                if csocs[i] == 'error':
                    connecting = True
        # print(name,'connected all')
        ssocs = {}
        addrs = []
        for i in range(int(numprocs)):
            s, addr = servsoc.accept()
            id = s.recv(1)
            conum = int.from_bytes(id,'big')
            ssocs[conum] = s
            addrs.append(addr)
        print(name,"found",addrs)

        liveProcs = {}
        for i in range(int(numprocs)):
            liveProcs[i] = True
    else:
        name = "p"+num
        print(name,"revived")
        myport = baseport + int(num)
        servsoc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servsoc.bind(('localhost',myport))
        servsoc.listen(int(numprocs))
        csocs = {}
        for i in range(int(numprocs)):
            port = baseport + i
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                c.connect(('localhost',port))
                csocs[i] = c
                c.send(int(num).to_bytes(1,'big'))
            except:
                csocs[i] = 'dead'
        #print(name,'found',csocs)
        ssocs = {}
        addrs = []
        for c in csocs:
            #print(c)
            if csocs[c] != 'dead':
                s, addr = servsoc.accept()
                id = s.recv(1)
                conum = int.from_bytes(id,'big')
                ssocs[conum] = s
                addrs.append(addr)
                #print(name,'counterfound',conum)
            else:
                ssocs[c] = 'dead'
                #print(name,'logs dead',c)
        #print(name,'connected back')

    coord = [0,0]

    Playlist = {}
    logfile = name + 'log.txt'
    playfile = name + 'playlist.dat'

    if not clean:
        log = open(playfile,'rb')
        Playlist = pickle.load(log)
        liveProcs = pickle.load(log)
        coord = pickle.load(log)
        log.close()
        # Update viewnumber
        vns = [coord[1]]
        for s in ssocs:
            if ssocs[s] != 'dead' and s != int(num):
                vns.append(int.from_bytes(ssocs[s].recv(1),'big'))
        if vns:
            coord[1] = max(vns)
            coord[0] = coord[1] % int(numprocs)

        # check validity of playlist against event log
        print(name,'consulting log')
        log = open(logfile, 'r')
        lines = []
        for line in log:
            lines.append(line)
        log.close()
        started = False
        finished = False
        trcont = ''
        trtype = -1
        if lines:
            if lines[0][:3] == 'add':
                started = True
                trcont = lines[0]
                trtype = 1
            elif lines[0][:6] == 'remove':
                started = True
                trcont = lines[0]
                trtype = 2
            elif lines[0][:4] == 'edit':
                started = True
                trcont = lines[0]
                trtype = 3
        if started:
            #print(trcont)
            for line in lines:
                if line == 'transaction done':
                    print(name,'found complete transaction')
                    finished = True
                    log = open(logfile, 'a')
                    log.write('transaction done')
                    log.close()
            if not finished:
                for line in lines:
                    if line.find('abort') != -1:
                        print(name,'found abort decision in incomplete transaction')
                        finished = True
                    elif line.find('commit') != -1:
                        print(name,'found (pre)commit decision in incomplete transaction')
                        if trtype == 1:
                            x = trcont[5:].find("\"")
                            song = trcont[5:x+5]
                            url = trcont[x+8:-2]
                            if song in Playlist:
                                if Playlist[song] == url:
                                    print(name,'playlist correct')
                                    finished = True
                            else:
                                Playlist[song] = url
                                print(name,'updated playlist')
                                finished = True
                        elif trtype == 2:
                            song = trcont[8:-2]
                            if song in Playlist:
                                del Playlist[song]
                                print(name,'updated playlist')
                                finished = True
                            else:
                                print(name,'playlist correct')
                                finished = True
                        elif trtype == 3:
                            x = trcont[6:].find("\"")
                            song = trcont[6:x+6]
                            y = trcont[x+9:].find("\"")
                            song2 = trcont[x+9:x+y+9]
                            url = trcont[x+y+12:-2]
                            if song2 in Playlist:
                                if Playlist[song2] == url:
                                    print(name,'playlist correct')
                                    finished = True
                            elif song in Playlist:
                                del Playlist[song]
                                Playlist[song2] = url
                                print(name,'updated playlist')
                                finished = True
                            else:
                                print(name,'seems to have an inconsistent playlist recorded')
                                finished = True
        else:
            print(name,'found nothing in the log')

            # ask other processes what happened if there was an incomplete transaction
        if started and not finished:
            print(name,'found incomplete transaction')
            idb = int(num).to_bytes(1,'big')
            if trtype == 1: # if its an and
                x = trcont[5:].find("\"")
                song = trcont[5:x+5]
                url = trcont[x+8:-2]
                msgba = b'\x0c'+idb+b'\x06'
                msgstrb = song + '\0' + url  # CONVERT TRANSACTION TO MESSAGE FORM
                msgbb = msgstrb.encode('utf8')
                msgb = msgba + msgbb
                leng = len(msgb)
                lenb = leng.to_bytes(1,'big')
                msg = lenb+msgb
            elif trtype == 2:
                song = trcont[8:-2]
                msgba = b'\x0c'+idb+b'\x07'
                msgstrb = song
                msgbb = msgstrb.encode('utf8')
                msgb = msgba + msgbb
                leng = len(msgb)
                lenb = leng.to_bytes(1,'big')
                msg = lenb+msgb
            elif trtype == 3:
                x = trcont[6:].find("\"")
                song = trcont[6:x+6]
                y = trcont[x+9:].find("\"")
                song2 = trcont[x+9:x+y+9]
                url = trcont[x+y+12:-2]
                msgba = b'\x0c'+idb+b'\x08'
                msgstrb = song + '\0' + song2 + '\0' + url
                msgbb = msgstrb.encode('utf8')
                msgb = msgba + msgbb
                leng = len(msgb)
                lenb = leng.to_bytes(1,'big')
                msg = lenb+msgb

            finalmessage = msg
            resolved = False
            for c in csocs:
                if c != int(name[1:]) and csocs[c] != 'dead':
                    csocs[c].send(msg)  # status request
                    print(name,'sent log inquiry to',c)
                    response = int.from_bytes(ssocs[c].recv(1),'big')
                    if response == 0:
                        print(name,'aborting logged transaction')
                        log = open(logfile, 'a')
                        log.write('told abort\n')
                        log.write('transaction done')
                        log.close()
                        resolved = True
                        break
                    elif response == 1:
                        print(name,'committing logged transaction')
                        if trtype == 1:
                            Playlist[song] = url
                            print(name,'updated playlist')
                        elif trtype == 2:
                            del Playlist[song]
                            print(name,'updated playlist')
                        elif trtype == 3:
                            del Playlist[song]
                            Playlist[song2] = url
                            print(name,'updated playlist')
                        log = open(logfile, 'a')
                        log.write('told commit\n')
                        log.write('transaction done')
                        log.close()
                        resolved = True
                        break
                    elif response == 2:
                        print(name,'talked to uncertain process')
                    else:
                        print(name,'somehow received an invalid response')

            # Last To Fail handling will go here (process can compare what was alive when it failed to what is alive now)
            if not resolved:
                print(name,'STILL HAS UNCERTAIN TRANSACTION IN LOG. LAST TO FAIL TERMINATION PROTOCOL NEEDED')
                numtocheck = 0
                for p in liveProcs:
                    if liveProcs[p]:
                        numtocheck += 1
                waiting = True
                while waiting:
                    try: # inline reviveReconnect sans liveProcs
                        check,b,c = select.select([servsoc],[],[],.05)
                        # print(check)
                        if check:
                            s, addr = servsoc.accept()
                            id = s.recv(1)
                            conum = int.from_bytes(id,'big')
                            ssocs[conum] = s
                            coname = 'p'+str(conum)
                            print(name,'found',coname)
                            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            c.connect(('localhost',baseport+conum))
                            csocs[conum] = c
                            c.send(int(name[1:]).to_bytes(1,'big'))
                            c.send(int(coord[1]).to_bytes(1,'big'))
                    except socket.timeout:
                        pass

                    inlist = []
                    [inlist.extend([k]) for k in ssocs.values()]
                    inlist[:]=[x for x in inlist if x != 'dead']
                    ready_to_read, b, c = select.select(inlist,[],[],0.05)
                    msgs = []
                    err = []
                    for sock in ready_to_read:
                        try:
                            msg = sock.recv(1)
                            if msg == b'':
                                for s in ssocs:
                                    if ssocs[s] == sock:
                                        ssocs[s].close()
                                        csocs[s].close()
                                        ssocs[s] = 'dead'
                                        csocs[s] = 'dead'
                                        err.append(s)
                            else:
                                leng = int.from_bytes(msg,'big')
                                msg = sock.recv(leng)
                                while len(msg)<leng:
                                    msg += sock.recv(leng-len(msg))
                                msgs.append(msg)
                        except:
                            # print("connection dropped",sock)
                            for s in ssocs:
                                if ssocs[s] == sock:
                                    ssocs[s].close()
                                    csocs[s].close()
                                    ssocs[s] = 'dead'
                                    csocs[s] = 'dead'
                                    err.append(s)
                    if err:
                        print(name,'lost connection to',err)

                    for msg in msgs:
                        code = msg[:1]
                        intcode = int.from_bytes(code,'big')
                        if intcode == 12:
                            print(name,'is lost in the uncertain zone')
                            csocs[int.from_bytes(id,'big')].send(b'\x02')
                        else:
                                print(name,"received irrelevant msg",msg)

                    check = True
                    for p in liveProcs:
                        if liveProcs[p] and csocs[p] == 'dead':
                            check = False
                    if check:
                        print(name,'last known live processes back up')
                        for p in liveProcs:
                            if liveProcs[p]:
                                csocs[p].send(finalmessage)
                                rspb = ssocs[p].recv(1)
                                if rspb in [b'\x00',b'\x01',b'\x02']:
                                    response = int.from_bytes(rspb,'big')
                                    if response == 0:
                                        print(name,'aborting logged transaction')
                                        log = open(logfile, 'a')
                                        log.write('told abort\n')
                                        log.write('transaction done')
                                        log.close()
                                        resolved = True
                                        break
                                    elif response == 1:
                                        print(name,'committing logged transaction')
                                        if trtype == 1:
                                            Playlist[song] = url
                                            print(name,'updated playlist')
                                        elif trtype == 2:
                                            del Playlist[song]
                                            print(name,'updated playlist')
                                        elif trtype == 3:
                                            del Playlist[song]
                                            Playlist[song2] = url
                                            print(name,'updated playlist')
                                        log = open(logfile, 'a')
                                        log.write('told commit\n')
                                        log.write('transaction done')
                                        log.close()
                                        resolved = True
                                        break
                                    elif response == 2:
                                        print(name,'talked to uncertain process')
                                        numtocheck -= 1
                                        if numtocheck == 0:
                                            print(name,'aborting logged transaction; all last to fail did not commit')
                                            log = open(logfile, 'a')
                                            log.write('inferred abort\n')
                                            log.write('transaction done')
                                            log.close()
                                            resolved = True
                                            break
                                    else:
                                        print(name,'somehow received an invalid response',msg)
                                else:
                                    msg = sock.recv(int.from_bytes(rspb,'big'))
                                    code = msg[1:2]
                                    intcode = int.from_bytes(code,'big')
                                    if intcode == 12:
                                        print(name,'is lost in the uncertain zone')
                                        csocs[int.from_bytes(id,'big')].send(b'\x02')
                                    else:
                                            print(name,"received irrelevant msg",msg)
                        waiting = False

        # Make notion of what is alive current
        for c in csocs:
            if csocs[c] != 'dead':
                liveProcs[c] = True
            else:
                liveProcs[c] = False
        print(name,'done reviving')

    #           0     1       2     3       4        5        6       7 partmsg 9    10      11      12
    comminfo=[csocs,ssocs,numprocs,name,liveProcs,playfile,Playlist,coord,-1,pipe,logfile,servsoc,baseport]

    running = True
    event = 0
    vote = True
    statuspoll=[]
    statuslist=[]
    statuses=[]

    logdata(comminfo)

    # process main loop

    while running:

        # check pipe from auto-controller
        stuff = pipe.poll()
        if stuff:
            cmd = pipe.recv()
            # print(name,cmd)
            if cmd[0] == 1:
                event = 1
            elif cmd[0] == 2:
                event = 2
            elif cmd[0] == 3:
                event = 3
            elif cmd[0] == 4:
                running = False
            elif cmd[0] == 5:
                print(name,'will cease 3pc messages after',cmd[1],'more sends')
                comminfo[8] = int(cmd[1])
            elif cmd[0] == 6:
                print(name,'will send 3pc messages as normal')
                comminfo[8] = -1
            elif cmd[0] == 8:
                print(name,'will reject the next vote')
                vote = False
            elif cmd[0] == 9:
                print(name,"has playlist",Playlist)
                print(name,'knows alive',liveProcs)
                print(name,'believes coordinator is',coord[0])
            else:
                print(name,"received invalid command",cmd,"exiting")
                running = False

        # Check for revived processes
        reviveReconnect(comminfo)

        # Second version, moving viewnum

        if int(num) == coord[0] and event > 0:

            # Second version: action given by controller
            goodtogo = True
            for i in range(int(numprocs)):
                if liveProcs[i] == False:
                    goodtogo = False
            if goodtogo:
                if event == 1:
                    add(cmd[1],cmd[2],comminfo,vote)
                    event = 0
                elif event == 2:
                    remove(cmd[1],comminfo,vote)
                    event = 0
                elif event == 3:
                    edit(cmd[1],cmd[2],cmd[3],comminfo,vote)
                    event = 0
                print(name,'done coordinating transaction')
                vote = True
            else:
                print('dead process, not going ahead with transaction')
                event = 0

        # check for waiting messages
        msgs,err = receive(comminfo)

        for e in err: # if a process dies while gathering states, disregard it
            if e in statuspoll:
                statuspoll.remove(e)

        if -1 in err and coord[0] == int(num):
            statuspoll = []
            print(name,'unaware of any transactions; requesting state from other processes')
            for c in comminfo[0]:
                if c != int(name[1:]) and comminfo[0][c] != 'dead':
                    err = send3pc(comminfo,comminfo[0][c],b'\x01\x0a')  # status request
                    if not err:
                        statuspoll.append(c)


        # print(name,"checked messages")

        for msg in msgs:
            code = msg[:1]
            intcode = int.from_bytes(code,'big')
            if intcode == 6:
                msgbody = msg[1:].decode('utf8')
                term = msgbody.find('\0')
                songname = msgbody[:term]
                url = msgbody[term+1:]
                logmsg = 'add \"'+songname+'\" \"'+url+'\"\n'
                log = open(comminfo[10], 'w')
                log.write(logmsg)
                log.close()
                print(name,'voting on ADD',songname,url)
                decision = vote3pc(comminfo,vote,msg)
                if decision:
                    Playlist[songname] = url
                    logdata(comminfo)
                log = open(comminfo[10], 'a')
                log.write('transaction done')
                log.close()
                vote = True
            elif intcode == 7:
                msgbody = msg[1:].decode('utf8')
                songname = msgbody
                logmsg = 'remove \"'+songname+'\"\n'
                log = open(comminfo[10], 'w')
                log.write(logmsg)
                log.close()
                print(name,'voting on REMOVE',songname)
                decision = vote3pc(comminfo,vote,msg)
                if decision:
                    del Playlist[songname]
                    logdata(comminfo)
                log = open(comminfo[10], 'a')
                log.write('transaction done')
                log.close()
                vote = True
            elif intcode == 8:
                msgbody = msg[1:].decode('utf8')
                term = msgbody.find('\0')
                songname = msgbody[:term]
                term2 = msgbody[term+1:].find('\0')
                songname2 = msgbody[term+1:term+term2+1]
                url = msgbody[term+term2+2:]
                logmsg = 'edit \"'+songname+'\" \"'+songname2+'\" \"'+url+'\"\n'
                log = open(comminfo[10], 'w')
                log.write(logmsg)
                log.close()
                print(name,'voting on EDIT',songname,songname2,url)
                decision = vote3pc(comminfo,vote,msg)
                if decision:
                    del Playlist[songname]
                    Playlist[songname2] = url
                    logdata(comminfo)
                log = open(comminfo[10], 'a')
                log.write('transaction done')
                log.close()
                vote = True
            elif intcode == 9:
                pass  # someone was testing connection liveness
            elif intcode == 10:
                idb = int(num).to_bytes(1,'big')
                msgb = b'\x0b'+idb+b'\x00' #reply with no active transaction
                leng = len(msgb)
                lenb = leng.to_bytes(1,'big')
                msg = lenb+msgb
                send3pc(comminfo,comminfo[0][comminfo[7][0]],msg)
            elif intcode == 11:
                msgbody = msg[1:]
                id = int.from_bytes(msgbody[:1],'big')
                statuslist.append(id)
                statuses.append(msgbody)
            elif intcode == 12:
                print(name,'received a log inquiry')
                id = int.from_bytes(msg[1:2],'big')
                msg2 = msg[2:]
                code2 = msg2[:1]
                intcode2 = int.from_bytes(code2,'big')
                if intcode2 == 6:
                    msgbody = msg2[1:].decode('utf8')
                    term = msgbody.find('\0')
                    songname = msgbody[:term]
                    url = msgbody[term+1:]
                    logmsg = 'add \"'+songname+'\" \"'+url+'\"\n'

                elif intcode2 == 7:
                    msgbody = msg[1:].decode('utf8')
                    songname = msgbody
                    logmsg = 'remove \"'+songname+'\"\n'

                elif intcode == 8:
                    msgbody = msg[1:].decode('utf8')
                    term = msgbody.find('\0')
                    songname = msgbody[:term]
                    term2 = msgbody[term+1:].find('\0')
                    songname2 = msgbody[term+1:term+term2+1]
                    url = msgbody[term+term2+2:]
                    logmsg = 'edit \"'+songname+'\" \"'+songname2+'\" \"'+url+'\"\n'

                else:
                    print(name,'encountered error processing log request')
                    logmsg = 'error'

                log = open(logfile, 'r')
                lines = []
                for line in log:
                    lines.append(line)
                log.close()
                if lines: # there must be a logged transaction
                    if logmsg == lines[0]: # it must be the RIGHT transaction
                        print(name,'recognizes the requested transaction')
                        resolved = False
                        for line in lines:
                            if line.find('abort') != -1:
                                print(name,'recorded abort decision')
                                csocs[id].send(b'\x00')
                                resolved = True
                                break
                            elif line.find('commit') != -1:
                                print(name,'recorded (pre)commit decision')
                                csocs[id].send(b'\x01')
                                resolved = True
                                break
                        if not resolved: # This honestly should NOT occur here, but I'm putting it in so I can reuse this whole section
                            print(name,'is uncertain about decision')
                            csocs[id].send(b'\x02')
                    else:
                        print(name,'does not recognize the requested transaction')
                        csocs[id].send(b'\x00')
                else:
                    print(name,'does not recognize the requested transaction')
                    csocs[id].send(b'\x00')
            else:
                    print(name,"received irrelevant msg",msg)

        if statuspoll:
            chk = True
            for s in statuspoll: #make sure all the statuses have been gathered
                if s not in statuslist:
                    chk = False
            for s in statuslist: # the process that gave this state has since died and should be disregarded
                if s not in statuspoll:
                    statuslist.remove(s)
            if chk:
                print(name,'checking state of other processes')
                statuspoll = []
                statuslist = []
                intrsv = [] # in transaction, voted
                intrsp = [] # in transaction, precommitted
                outrs = [] # out of transaction
                for s in statuses:
                    if s[1:2] == b'\x01':
                        if s [2:3] == b'\x02': # SOMEBODY HAS RECEIVED A PRECOMMIT, MOVE FORWARD
                            intrsp.append(int.from_bytes(s[0:1],'big'))
                        else: # received req but not precommit
                            intrsv.append(int.from_bytes(s[0:1],'big'))
                    else: # not in transaction
                        outrs.append(int.from_bytes(s[0:1],'big'))
                if intrsp:
                    print(name,'sending COMMIT from outside current transaction')
                    for c in intrsp: # send commit to those unaware
                        send3pc(comminfo,comminfo[0][c],b'\x01\x05')  # commit code
                elif intrsv:
                    print(name,'sending ABORT from outside current transaction')
                    for c in intrsv: # sending abort to only the processes that were notified of the transaction
                        send3pc(comminfo,comminfo[0][c],b'\x01\x04')  # abort code
                else:
                    print('no active transactions') # no further action



    # Post main-loop cleanup

    for i in range(int(numprocs)):
        if csocs[i] != 'dead':
            csocs[i].close()
    print(name,"has playlist",Playlist)
    print(name,'exiting')


def add(songname,url,comminfo,vote):
    msgstr = '\6' + songname + '\0' + url  # add a null to check for end of string
    leng = len(msgstr)
    msgb = msgstr.encode('utf8')
    lenb = leng.to_bytes(1,'big')
    msg = lenb+msgb
    logmsg = 'add \"'+songname+'\" \"'+url+'\"\n'
    log = open(comminfo[10], 'w')
    log.write(logmsg)
    log.close()
    print(comminfo[3],'coordinating ADD',songname,url)
    decision = coord3pc(comminfo,vote,msg)
    if decision:
        comminfo[6][songname] = url
        logdata(comminfo)
    log = open(comminfo[10], 'a')
    log.write('transaction done')
    log.close()


def remove(songname,comminfo,vote):
    msgstr = '\7' + songname
    leng = len(msgstr)
    msgb = msgstr.encode('utf8')
    lenb = leng.to_bytes(1,'big')
    msg = lenb+msgb
    logmsg = 'remove \"'+songname+'\"\n'
    log = open(comminfo[10], 'w')
    log.write(logmsg)
    log.close()
    print(comminfo[3],'coordinating REMOVE',songname)
    decision = coord3pc(comminfo,vote,msg)
    if decision:
        del comminfo[6][songname]
        logdata(comminfo)
    log = open(comminfo[10], 'a')
    log.write('transaction done')
    log.close()


def edit(songname,newsongname,newurl,comminfo,vote):
    msgba = b'\x08'
    msgstrb = songname + '\0' + newsongname + '\0' + newurl  # add a null to check for end of string
    msgbb = msgstrb.encode('utf8')
    msgb = msgba + msgbb
    leng = len(msgb)
    lenb = leng.to_bytes(1,'big')
    msg = lenb+msgb
    logmsg = 'edit \"'+songname+'\" \"'+newsongname+'\" \"'+newurl+'\"\n'
    log = open(comminfo[10], 'w')
    log.write(logmsg)
    log.close()
    print(comminfo[3],'coordinating EDIT',songname,newsongname,newurl)
    decision = coord3pc(comminfo,vote,msg)
    if decision:
        del comminfo[6][songname]
        comminfo[6][newsongname] = newurl
        logdata(comminfo)
    log = open(comminfo[10], 'a')
    log.write('transaction done')
    log.close()


def coord3pc(comminfo,vote,vrq):
    name = comminfo[3]
    failed = False
    partreq = []
    print(name,'sending VOTE-REQs')
    for c in comminfo[0]:
        if c != int(comminfo[3][1:]):
            print(name,'sending VOTE-REQ to',c)
            err = send3pc(comminfo,comminfo[0][c],vrq)
            if err:
                failed = True
                print(name,'ABORTing transaction')
                break
            else:
                partreq.append(c)
    if failed:
        log = open(comminfo[10], 'a')
        log.write('decided abort\n')
        log.close()
        print(name,'sending ABORT')
        if partreq:
            for c in partreq: # sending abort to only the processes that were notified of the transaction
                send3pc(comminfo,comminfo[0][c],b'\x01\x04')  # abort code
        return
    if vote:
        log = open(comminfo[10], 'a')
        log.write('voting yes\n')
        log.close()
        print(name,'rec YES from self')
    else:
        log = open(comminfo[10], 'a')
        log.write('voting no and aborting\n')
        log.close()
        failed = True
        print(name,'rec NO from self')
    if failed:
        log = open(comminfo[10], 'a')
        log.write('decided abort\n')
        log.close()
        print(name,'sending ABORT')
        for c in comminfo[0]:
            if c != int(name[1:]) and comminfo[0][c] != 'dead':
                send3pc(comminfo,comminfo[0][c],b'\x01\x04')  # abort code
        return False
    return coord3pc1(comminfo,0)


def coord3pc1(comminfo,aware):
    name = comminfo[3]
    print(name,'entered coord3pc1')
    failed = False
    numyes = aware + 1
    print(name,'collecting VOTEs')
    numlive = 0
    for p in comminfo[4]:
        if comminfo[4][p]:
            numlive += 1
    while numyes < numlive and not failed:
        msgs,err = receive(comminfo)
        for msg in msgs:
            code = msg[:1]
            intcode = int.from_bytes(code,'big')
            if intcode == 0:
                print(name,'rec NO')
                failed = True
            elif intcode == 1:
                numyes += 1
                print(name,'rec YES num',numyes)
            else:
                print(name,"received irrelevant msg",msg)
        if err:
            failed = True #error here means not all votes are known but no one could have committed. safe to abort
            print(name,'ABORTing transaction')
    if failed:
        log = open(comminfo[10], 'a')
        log.write('decided abort\n')
        log.close()
        print(name,'sending ABORT')
        for c in comminfo[0]:
            if c != int(name[1:]) and comminfo[0][c] != 'dead':
                send3pc(comminfo,comminfo[0][c],b'\x01\x04')  # abort code
        return False
    else:
        log = open(comminfo[10], 'a')
        log.write('sending precommit\n')
        log.close()
        print(name,'sending PRECOMMIT')
        for c in comminfo[0]:
            if c != int(name[1:]) and comminfo[0][c] != 'dead':
                send3pc(comminfo,comminfo[0][c],b'\x01\x02')  # precommit code
    return coord3pc2(comminfo,0)


def coord3pc2(comminfo,aware): # at this point coordinator knows all votes were yes and precommit was sent
    name = comminfo[3]
    print(name,'entered coord3pc2')
    failed = False
    print(name,'rec PRECOMMIT from self')
    numack = 1 + aware
    print(name,'rec ACK from self')
    numlive = 0
    for p in comminfo[4]:
        if comminfo[4][p]:
            numlive += 1
    while numack < numlive and not failed: # needs acknowledgment from all live processes
        msgs,err = receive(comminfo)
        for msg in msgs:
            code = msg[:1]
            intcode = int.from_bytes(code,'big')
            if intcode == 3:
                numack += 1
                print(name,'rec ACK num',numack)
            else:
                print(name,"received irrelevant msg",msg)
        if err:
            failed = True
            print(name,'lost connection to',err) #error here means all votes were yes (no aborts), however some processes may still be uncertain
    if not failed:
        log = open(comminfo[10], 'a')
        log.write('deciding commit\n')
        log.close()
        print(name,'sending COMMIT')
        for c in comminfo[0]:
            if c != int(name[1:]) and comminfo[0][c] != 'dead':
                send3pc(comminfo,comminfo[0][c],b'\x01\x05')  # commit code
        print(name,'rec COMMIT from self')
        return True
    else: # everyone voted yes, go ahead with commit anyway
        log = open(comminfo[10], 'a')
        log.write('deciding commit after participant crash\n')
        log.close()
        print(name,'sending COMMIT')
        for c in comminfo[0]:
            if c != int(name[1:]) and comminfo[0][c] != 'dead':
                send3pc(comminfo,comminfo[0][c],b'\x01\x05')  # commit code
        print(name,'rec COMMIT from self')
        return True


def vote3pc(comminfo,vote,vrq):
    name = comminfo[3]
    num = int(name[1:])
    if vote:
        log = open(comminfo[10], 'a')
        log.write('voting yes\n')
        log.close()
        send3pc(comminfo,comminfo[0][comminfo[7][0]],b'\x01\x01') #yes
        print(name,'votes YES')
    else:
        log = open(comminfo[10], 'a')
        log.write('voting no\n')
        log.close()
        send3pc(comminfo,comminfo[0][comminfo[7][0]],b'\x01\x00') #no
        print(name,'votes NO')
    checking = True
    while checking:
        msgs,err = receive(comminfo)
        for msg in msgs:
            code = msg[:1]
            intcode = int.from_bytes(code,'big')
            if intcode == 2:
                log = open(comminfo[10], 'a')
                log.write('received precommit\n')
                log.close()
                print(name,'rec PRECOMMIT')
                checking = False
            elif intcode == 4:
                log = open(comminfo[10], 'a')
                log.write('received abort\n')
                log.close()
                print(name,'rec ABORT')
                return False
            else:
                print(name,"received irrelevant msg",msg)

        if err:
            if -1 in err: # coordinator died
                if int(comminfo[3][1:]) != comminfo[7][0]: # this process is NOT the new coordinator
                    checking2 = True
                    if vote:
                        voteb = b'\x01'
                    else:
                        voteb = b'\x00'
                    while checking2:
                        msgs,err = receive(comminfo)
                        for msg in msgs:
                            code = msg[:1]
                            intcode = int.from_bytes(code,'big')
                            if intcode == 10:
                                idb = int(num).to_bytes(1,'big')
                                msgb = b'\x0b'+idb+b'\x01'+voteb+vrq #reply with active transaction
                                leng = len(msgb)
                                lenb = leng.to_bytes(1,'big')
                                msg = lenb+msgb
                                send3pc(comminfo,comminfo[0][comminfo[7][0]],msg)
                                checking2 = False
                            else:
                                print(name,"received irrelevant msg",msg)
                        if err:
                            if -1 in err: # coordinator died
                                if int(comminfo[3][1:]) != comminfo[7][0]:
                                    pass # stay in this loop
                                else:
                                    return v3pcnc(comminfo,vrq)
                else: # this process IS the new coordinator
                    return v3pcnc(comminfo,vrq)
            else: # some non-coordinator process died. doesn't affect this process directly
                pass
    return vote3pc2(comminfo)


def v3pcnc(comminfo,vrq):
    name = comminfo[3]
    statuspoll = []
    statuslist=[]
    statuses=[]
    print(name,'in transaction; requesting state from other processes')
    for c in comminfo[0]:
        if c != int(name[1:]) and comminfo[0][c] != 'dead':
            err = send3pc(comminfo,comminfo[0][c],b'\x01\x0a')  # status request
            if not err:
                statuspoll.append(c)
    polling = True
    while polling:
        msgs,err = receive(comminfo)

        for msg in msgs:
            code = msg[:1]
            intcode = int.from_bytes(code,'big')
            if intcode == 11:
                msgbody = msg[1:]
                id = int.from_bytes(msgbody[:1],'big')
                statuslist.append(id)
                statuses.append(msgbody)
            else:
                print(name,"received irrelevant msg",msg)

        for e in err: # if a process dies while gathering states, disregard it
            if e in statuspoll:
                statuspoll.remove(e)

        chk = True
        for s in statuspoll: #make sure all the statuses have been gathered
            if s not in statuslist:
                chk = False
        for s in statuslist: # the process that gave this state has since died and should be disregarded
            if s not in statuspoll:
                statuslist.remove(s)
        if chk:
            polling = False
            decision = 0 # 0 - no precommits, abort; 1 - at least one precommit, go for commit
            print(name,'checking state of other processes')
            intrsv = [] # in transaction, voted
            intrsp = [] # in transaction, precommitted
            outrs = [] # out of transaction
            for s in statuses:
                if s[1:2] == b'\x01':
                    if s [2:3] == b'\x02': # SOMEBODY HAS RECEIVED A PRECOMMIT, MOVE FORWARD
                        intrsp.append(int.from_bytes(s[0:1],'big'))
                        decision = 1
                    else: # received req but not precommit
                        intrsv.append(int.from_bytes(s[0:1],'big'))
                else: # not in transaction
                    outrs.append(int.from_bytes(s[0:1],'big'))
            if decision == 1: # precommit found
                print(name,'REsending PRECOMMIT;',len(intrsp),'processes already precommitted')
                for c in intrsv: # send precommit to those unaware
                    send3pc(comminfo,comminfo[0][c],b'\x01\x02')  # precommit code
                # no error check, if I lose them, I'm pushing ahead anyway
                return coord3pc2(comminfo,len(intrsp))
            else: # need to send the vote-req to all unaware processes, tally votes and then enter regular coord3pc2
                log = open(comminfo[10], 'a')
                log.write('decided abort\n')
                log.close()
                print(name,'sending ABORT')
                for c in intrsv: # sending abort to only the processes that were notified of the transaction
                    send3pc(comminfo,comminfo[0][c],b'\x01\x04')  # abort code
                return False


def vote3pc2(comminfo):
    name = comminfo[3]
    num = int(name[1:])
    send3pc(comminfo,comminfo[0][comminfo[7][0]],b'\x01\x03') #ack
    print(name,'ACKs')
    checking = True
    while checking:
        msgs,err = receive(comminfo)
        for msg in msgs:
            code = msg[:1]
            intcode = int.from_bytes(code,'big')
            if intcode == 5:
                log = open(comminfo[10], 'a')
                log.write('received commit\n')
                log.close()
                print(name,'rec COMMIT')
                return True
            else:
                print(name,"received irrelevant msg",msg)
        if err:
            print(name,'lost connection to',err) #process knows all other processes have voted yes, safe to commit
            if -1 in err: # coordinator died
                if int(comminfo[3][1:]) != comminfo[7][0]: # this process is NOT the new coordinator
                    checking2 = True
                    while checking2:
                        msgs,err = receive(comminfo)
                        for msg in msgs:
                            code = msg[:1]
                            intcode = int.from_bytes(code,'big')
                            if intcode == 10:
                                idb = int(num).to_bytes(1,'big')
                                msgb = b'\x0b'+idb+b'\x01\x02' #reply with active transaction
                                leng = len(msgb)
                                lenb = leng.to_bytes(1,'big')
                                msg = lenb+msgb
                                send3pc(comminfo,comminfo[0][comminfo[7][0]],msg)
                                checking2 = False
                            else:
                                print(name,"received irrelevant msg",msg)
                        if err:
                            if -1 in err: # coordinator died
                                if int(comminfo[3][1:]) != comminfo[7][0]:
                                    pass # stay in this loop
                                else:
                                    return v3pcnc2(comminfo)
                else: # this process IS the new coordinator, (it does not know if anyone else has received precommit so it has to start over there)
                    return v3pcnc2(comminfo)
            else: # some non-coordinator process died. doesn't affect this process directly
                pass
    print(name,'SHOULD NOT HAVE REACHED THIS MESSAGE')
    return False


def v3pcnc2(comminfo):
    name = comminfo[3]
    statuspoll = []
    statuslist=[]
    statuses=[]
    print(name,'in transaction; requesting state from other processes')
    for c in comminfo[0]:
        if c != int(name[1:]) and comminfo[0][c] != 'dead':
            err = send3pc(comminfo,comminfo[0][c],b'\x01\x0a')  # status request
            if not err:
                statuspoll.append(c)
    polling = True
    while polling:
        msgs,err = receive(comminfo)

        for msg in msgs:
            code = msg[:1]
            intcode = int.from_bytes(code,'big')
            if intcode == 11:
                msgbody = msg[1:]
                id = int.from_bytes(msgbody[:1],'big')
                statuslist.append(id)
                statuses.append(msgbody)
            else:
                print(name,"received irrelevant msg",msg)

        for e in err: # if a process dies while gathering states, disregard it
            if e in statuspoll:
                statuspoll.remove(e)

        chk = True
        for s in statuspoll: #make sure all the statuses have been gathered
            if s not in statuslist:
                chk = False
        for s in statuslist: # the process that gave this state has since died and should be disregarded
            if s not in statuspoll:
                statuslist.remove(s)
        if chk:
            polling = False
            decision = 0 # 0 - everyone has already gotten a precommit; 1 - some processes still need to be informed
            print(name,'checking state of other processes')
            intrsv = [] # in transaction, voted
            intrsp = [] # in transaction, precommitted
            outrs = [] # out of transaction
            for s in statuses:
                if s[1:2] == b'\x01':
                    if s [2:3] == b'\x00': # no vote, REALLY OUGHT TO NEVER ENTER THIS
                        intrsv.append(int.from_bytes(s[0:1],'big'))
                        print(name,'FOUND A NO VOTE AFTER PRECOMMITTING')
                    elif s [2:3] == b'\x02': # SOMEBODY HAS RECEIVED A PRECOMMIT, MOVE FORWARD
                        intrsp.append(int.from_bytes(s[0:1],'big'))
                    else: # yes vote
                        intrsv.append(int.from_bytes(s[0:1],'big'))
                        decision = 1
                else: # not in transaction
                    outrs.append(int.from_bytes(s[0:1],'big'))
            if decision == 1:
                print(name,'REsending PRECOMMIT;',len(intrsp),'processes already precommitted')
                for c in intrsv: # send precommit to those unaware
                    send3pc(comminfo,comminfo[0][c],b'\x01\x02')  # precommit code
                # no error check, if I lose them, I'm pushing ahead anyway
                return coord3pc2(comminfo,len(intrsp))
            else:
                for c in intrsp: # send commit to those unaware
                    send3pc(comminfo,comminfo[0][c],b'\x01\x05')  # commit code
                return True


def receive(cominf):
    inlist = []
    [inlist.extend([k]) for k in cominf[1].values()]
    inlist[:]=[x for x in inlist if x != 'dead']
    # print(cominf[3],inlist)
    ready_to_read, b, c = select.select(inlist,[],[],0.05)
    msgs = []
    conerr = False
    err = []
    for sock in ready_to_read:
        try:
            msg = sock.recv(1)
            if msg == b'':
                err += brokenconnection(cominf,sock,'receive')
            else:
                leng = int.from_bytes(msg,'big')
                msg = sock.recv(leng)
                while len(msg)<leng:
                    msg += sock.recv(leng-len(msg))
                msgs.append(msg)
        except:
            # print("connection dropped",sock)
            err += brokenconnection(cominf,sock,'receive')
    if err:
        print(cominf[3],'lost connection to',err)
    return [msgs,err]


def send3pc(cominf,target,msg):
    #print(cominf[3],'has partmsg',cominf[8])
    err = []
    if cominf[8] != 0:
        try:
            target.send(msg)
        except:
            err = brokenconnection(cominf,target,'send')
        if cominf[8] != -1:
            cominf[8] -= 1
    else:
        print(cominf[3],'pausing 3pc messages')
        paused = True
        while paused:
            stuff = cominf[9].poll()
            if stuff:
                cmd = cominf[9].recv()
                # print(name,cmd)
                if cmd[0] in[1,2,3]:
                    print(cominf[3],'cannot coordinate a new transaction while paused in the current one')
                elif cmd[0] == 4:
                    print('unpause',cominf[3],'so that it can resolve the current transaction before ending')
                elif cmd[0] == 5:
                    print(cominf[3],'will send',cmd[1],'additional 3pc messages')
                    cominf[8] = int(cmd[1])
                    paused = False
                elif cmd[0] == 6:
                    print(cominf[3],'will resume 3pc messaging')
                    cominf[8] = -1
                    paused = False
                elif cmd[0] == 8:
                    print(cominf[3],'cannot decide on the next transaction while paused in the current one')
                elif cmd[0] == 9:
                    print(cominf[3],'is paused in a 3pc transaction')
                else:
                    print(cominf[3],"received invalid command",cmd)
            #reviveReconnect(cominf) # so that the process can connect to reviving processes
        try:
            target.send(msg)
            print(cominf[3],'sent prior paused message')
        except:
            err = brokenconnection(cominf,target,'send')
    if err:
        print(cominf[3],'lost connection to',err)
    return err


def reviveReconnect(cominf):
    try:
        check,b,c = select.select([cominf[11]],[],[],.05)
        # print(check)
        if check:
            s, addr = cominf[11].accept()
            id = s.recv(1)
            conum = int.from_bytes(id,'big')
            cominf[1][conum] = s
            coname = 'p'+str(conum)
            print(cominf[3],'found',coname)
            cominf[4][conum] = True
            c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            c.connect(('localhost',cominf[12]+conum))
            cominf[0][conum] = c
            c.send(int(cominf[3][1:]).to_bytes(1,'big'))
            c.send(int(cominf[7][1]).to_bytes(1,'big'))
    except socket.timeout:
        pass


def brokenconnection(cominf,badconn,type):
    err = []
    if type == 'send':
        check = 0
    elif type == 'receive':
        check = 1
    else:
        print('bad type given to brokenconnection')
        return []
    for s in cominf[check]:
        if cominf[check][s] == badconn:
            cominf[1][s].close()
            cominf[0][s].close()
            cominf[1][s] = 'dead'
            cominf[0][s] = 'dead'
            cominf[4][s] = False
            logdata(cominf)
            err.append(s)
            if s == cominf[7][0]:
                print(cominf[3],'lost connection to coordinator, electing new one')
                electing = True
                while electing:
                    cominf[7][1] += 1
                    cominf[7][0] = cominf[7][1] % int(cominf[2])
                    if cominf[4][cominf[7][0]]:
                        print(cominf[3],'decides new coordinator is',cominf[7][0])
                        electing = False
                        err.append(-1)
    return err


def logdata(cominf):
    log = open(cominf[5],'wb')
    pickle.dump(cominf[6],log,2)
    pickle.dump(cominf[4],log,2)
    pickle.dump(cominf[7],log,2)
    log.close()


if __name__ == '__main__':
    running = True
    procs = {}
    liveProcs = {}
    coord = [0,0]
    numprocs = 0
    while running:
        next = input("Next command: ")

        if next[:2] == "cp":
            numprocs = int(next[3:])
            for i in range(numprocs):
                num = str(i)
                name = "p"+num
                logname = name + 'log.txt'
                playname = name + 'playlist.dat'
                clear = open(logname, 'w')
                clear.close()
                clear = open(playname, 'w')
                clear.close()
                here, there = Pipe()
                prss = Process(target=proc,args=(num,str(numprocs),there,True))
                prss.start()
                procs[name] = [prss, here]
                liveProcs[name] = True
                time.sleep(.05)
                coord = [0,0]

        elif next[:7] == "killAll":
            for name in procs:
                procs[name][0].terminate()
                liveProcs[name] = False
                # NOT SURE WHAT TO DO WITH COORDINATOR SUCCESSION
                # terminates happen fast enough that this is essentially simultaneous with regard to the terminated processes
                # I guess consider them all concurently the last to die

        elif next[:10] == "killLeader":
            name = 'p'+str(coord[0])
            procs[name][0].terminate()
            liveProcs[name] = False
            if int(name[1:]) == coord[0]:
                inlist = []
                [inlist.extend([k]) for k in liveProcs.values()]
                if max(inlist):
                    findingnext = True
                    while findingnext:
                        coord[1] += 1
                        coord[0] = coord[1] % numprocs
                        if liveProcs['p'+str(coord[0])]:
                            findingnext = False
                            print('new controller is','p'+str(coord[0]),'- ac')
                else:
                    print(name,'was last to die - ac')

        elif next[:4] == "kill":
            name = next[5:]
            procs[name][0].terminate()
            liveProcs[name] = False
            if int(name[1:]) == coord[0]:
                inlist = []
                [inlist.extend([k]) for k in liveProcs.values()]
                if max(inlist):
                    findingnext = True
                    while findingnext:
                        coord[1] += 1
                        coord[0] = coord[1] % numprocs
                        if liveProcs['p'+str(coord[0])]:
                            findingnext = False
                            print('new controller is','p'+str(coord[0]),'- ac')
                else:
                    print(name,'was last to die - ac')

        elif next[:9] == "reviveAll":
            pass

        elif next[:10] == "reviveLast":
            pass

        elif next[:6] == "revive":
            name = next[7:]
            num = name[1:]
            logname = name + 'log.txt'
            playname = name + 'playlist.dat'
            here, there = Pipe()
            prss = Process(target=proc,args=(num,str(numprocs),there,False))
            prss.start()
            procs[name] = [prss, here]
            liveProcs[name] = True
            time.sleep(.05)

        elif next[:7] == "partmsg":
            x = next[8:].find(" ")
            name = next[8:x+8]
            num = next[x+9:]
            cmd = [5,num]
            procs[name][1].send(cmd)

        elif next[:9] == "resumemsg":
            name = next[10:]
            num = name[1:]
            cmd = [6]
            procs[name][1].send(cmd)

        elif next[:10] == "rejectNext":
            name = next[11:]
            num = name[1:]
            cmd = [8]
            procs[name][1].send(cmd)

        elif next[:3] == "add":
            x = next[5:].find("\"")
            song = next[5:x+5]
            url = next[x+8:-1]
            cmd = [1,song,url]
            procs['p'+str(coord[0])][1].send(cmd)

        elif next[:6] == "remove":
            song = next[8:-1]
            cmd = [2,song]
            procs['p'+str(coord[0])][1].send(cmd)

        elif next[:4] == "edit":
            x = next[6:].find("\"")
            song = next[6:x+6]
            y = next[x+9:].find("\"")
            song2 = next[x+9:x+y+9]
            url = next[x+y+12:-1]
            cmd = [3,song,song2,url]
            procs['p'+str(coord[0])][1].send(cmd)

        elif next[:3] == "end":
            for name in procs:
                procs[name][1].send([4])
            running = False

        elif next[:6] == "status":
            print('autocontroller says coordinator is',coord[0])
            for name in procs:
                list = procs[name]
                list[1].send([9])

        elif next[:4] == "wait":
            amount = int(next[5:])
            time.sleep(amount)

        else:
            print("Invalid command")
    print('controller exiting')
