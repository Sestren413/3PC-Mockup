This project, like my last one was made on windows using python 3.5.0

As best as I can tell from my testing, the code I put together can handle everything up to but NOT INCLUDING total failure. I ran out of time before I could get that to work. It also can get stuck if a process is killed and then revived while the coordinator is paused although rekilling the process and then killing the coordinator should unstick things.

The project is using an automated controller that takes the following arguments and communicates with the processes through pipes:

cp <number> - creates that many new processes

killAll - terminates all running processes immediately

killLeader - terminates the current coordinator

kill <process> - terminates the specified process; the processes are p0 through pn

revive <process> - recreates the process. It looks in stable storage to rebuild its playlist

partmsg <process> <number> - has the process pause right before sending the n+1th message

resumemsg <process> - removes the effects of the previous command

rejectNext <process> - vote no on the next proposed transaction

status - all processes that are able print out a short message covering their current playlist, who they think is alive and who they think the coordinator is

end - tells the processes to end execution without terminating them (if you ever want that for some reason)


add "song" "url"
remove "song"
edit "song" "song2" "url"

the quotes are needed because the song names can handle having spaces

Each process stores some data in a binary file in addition to the text log. This additional data includes the playlist and the list of processes the process knows are alive.