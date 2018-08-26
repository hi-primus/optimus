# Optimus Server

The Optimus profiler let you expose your data stats via a little http server. Our goal here is to send this data to a GUI in the client machine.
Actually we are testing this concept with [bumblebee](https://github.com/ironmussa/Bumblebee).

To run the server:
```
python server.py
```  
You can create a config.ini file that let you specify where to output the profiler file and from which location the server is going to read the file.

```
[SERVER]
Input = ../data.json

[PROFILER]
Output = ../data.json
```
