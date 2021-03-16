from datetime import datetime
from influxdb import InfluxDBClient

class nmon:
    ZZZZ = {}
    lines = []
    sections = []
    hostname = ''
    version = ''
    fileneme = ''
    datapoints = []

    dbhost = ''
    dbport = 0
    dbname = ''
    dbuser = ''
    dbpassword = ''

    client = ''
    batchsize = 20000

    def __init__(self, filename: str, dbhost: str, dbport: int, dbname: str, dbuser: str, dbpassword: str):
        # init the object and create connection to the databas
        self.filename = filename
        self.dbhost = dbhost
        self.dbport = dbport
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbpassword = dbpassword

        try:
            self.client = InfluxDBClient(host=self.dbhost, port=self.dbport, database=self.dbname, username=self.dbuser, password=self.dbpassword)
            self.client.switch_database(self.dbname)
        except Exception as e:
            print("Oops!", e.__class__, "occurred.")

    def addDataPoint(self, datapoint):
        # add the datapoint and send to influx if above the batchsize
        self.datapoints.append(datapoint)

        if len(self.datapoints) % self.batchsize == 0:
            self.writeDataPoints()


        return

    def writeDataPoints(self):
        # flush all datapoints to influx
        print('Inserting %d datapoints...'%(len(self.datapoints)))
            
        response = self.client.write_points(self.datapoints,  protocol ="line")    

        if response == False:
            print('Problem inserting points, exiting...')
            exit(1)

        print("Wrote %d, response: %s" % (len(self.datapoints), response))

        self.datapoints = []       

        return

    def readFile(self):
        linesall = open(self.filename, 'r').read().splitlines()

        # split all lines on , and set the value
        for line in linesall:    
            self.lines.append(line.split(','))
        
        # gather system information
        for line in self.lines:    
            if (line[0] == "AAA"):            

                if (line[1] == "host"):
                    self.hostname = line[2]

                if (line[1] == "version"):
                    self.version = line[2]

            if not line[0] == "AAA" and not line[0] == "ZZZZ":
                # and build list of sections in this file
                if (line[0] not in self.sections):
                    self.sections.append(line[0])

        print("Hostname: %s " %  (self.hostname))
        print("NMON version: %s " % (self.version))

        # gather time stamps and convert them to epoch
        self.ZZZZ = {}
        for line in self.lines:    
            if (line[0] == "ZZZZ"):

                # calculate epoch
                epoch = datetime.strptime("%s %s" % (line[3], line[2]), '%d-%b-%Y %H:%M:%S').timestamp()

                self.ZZZZ[line[1]] = epoch * 1000 * 1000 * 1000

        print(self.ZZZZ['T0001'])

    def importSectionValues(self, measurement: str, tag: str, section: str):
        # This is used for the sections:
        # 
        # CPUxxx
        # CPU_ALL
        # MEM
        # PROC
        # VM

        headers = None
        # now loop through the lines and work on CPU_ALL
        for line in self.lines:
            if (line[0] == section):
                if headers is None:
                    print("header found for %s: %s" % (section, line))     
                
                    headers = line
                else:

                    datapoint = "%s,hostname=%s" % (measurement, self.hostname)

                    # add tag if given
                    if tag:
                        datapoint += ",%s" % (tag)

                    datapoint += " "

                    # add the fields with the values
                    for i in range(2, len(line)):
                        if line[i]:
                            datapoint += "%s=%s," % (headers[i], line[i])

                    # remove last , from the line
                    datapoint = datapoint[:-1]

                    datapoint += " %d" % (self.ZZZZ[line[1]])

                    self.addDataPoint(datapoint)

                    

        return

    def importSectionTagValues(self, measurement: str, tag: str, valuename: str, section: str):
        # this is used for the sections
        #
        # DISKBUSY
        # DISKREAD
        # DISKWRITE
        # DISKXFER
        # DISKBSIZE

        headers = None
        # now loop through the lines and work on this section
        for line in self.lines:
            if (line[0] == section):
                if headers is None:
                    print("header found for %s: %s" % (section, line))     
                
                    headers = line
                else:

                    # add the fields with the values
                    for i in range(2, len(line)):
                        datapoint = "%s,hostname=%s,%s=%s " % (measurement, self.hostname, tag, headers[i])

                        #if line[i]:
                        datapoint += "%s=%s " % (valuename, line[i])

                    

                        datapoint += " %d" % (self.ZZZZ[line[1]])

                        self.addDataPoint(datapoint)
                    
                    

        return

    def importSectionTagSplitValues(self, measurement: str, section: str, splitvalue: int):
        # this is used for the sections
        #
        # NET
        # NETPACKETS

        headers = None
        # now loop through the lines and work on this section
        for line in self.lines:
            if (line[0] == section):
                if headers is None:
                    print("header found for %s: %s" % (section, line))     
                
                    headers = line
                else:

                    # add the fields with the values
                    for i in range(2, len(line)):
                        interface_name = '-'.join(headers[i].split('-')[:splitvalue])
                        value_name = '-'.join(headers[i].split('-')[splitvalue:])
                        datapoint = "%s,hostname=%s,interface=%s " % (measurement, self.hostname, interface_name)
                        datapoint += "%s=%s" % (value_name, line[i])
                        datapoint += " %d" % (self.ZZZZ[line[1]])

                        self.addDataPoint(datapoint)

        return

    def importTop(self, measurement: str, section: str):
        # this is used for the sections
        #
        # TOP

        # TOP,+PID,Time,%CPU,%Usr,%Sys,Size,ResSet,ResText,ResData,ShdLib,MinorFault,MajorFault,Command

        headers = None
        # now loop through the lines and work on this section
        for line in self.lines:
            if (line[0] == section):
                if headers is None:
                    # alternative construction for TOP because it has two headers in the nmon file:
                    #
                    # TOP,%CPU Utilisation
                    # TOP,+PID,Time,%CPU,%Usr,%Sys,Size,ResSet,ResText,ResData,ShdLib,MinorFault,MajorFault,Command
                    
                    if not line[1] == "%CPU Utilisation":
                        print("header found for %s: %s" % (section, line))     
                    
                        headers = line
                else:
                    # init datapoint
                    datapoint = ''

                    # init tags
                    tags = []
                    tags.append("hostname=%s" % (self.hostname))

                    # add the fields with the values
                    for i in range(1, len(line)):
                        if headers[i] == "Command":
                            # use as tag
                            tags.append("process=%s" % (line[i]))                            
                        elif headers[i] == "+PID":
                            # use as tag as well
                            tags.append("pid=%s" % (line[i]))
                        elif headers[i] == "Time":
                            pass
                        else:
                            if line[i]:
                                datapoint += "%s=%s," % (headers[i], line[i])
                        
                    # add the measurement tag upfront
                    datapoint = "%s,%s %s" % (measurement, ','.join(tags), datapoint)

                    # remove last , from the line
                    datapoint = datapoint[:-1]
                    
                    # and add the timestamp
                    datapoint += " %d" % (self.ZZZZ[line[2]])

                    #print(datapoint)
                    self.addDataPoint(datapoint)                        

        return
    

    def showSections(self):
        return self.sections
