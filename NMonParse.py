from datetime import datetime
import glob
import argparse
from influxdb import InfluxDBClient
import NMon

# parse the arguments
parser = argparse.ArgumentParser()

parser.add_argument('--filename', help='NMon logfile to process - wildcard possible', required=True)
parser.add_argument('--dbhost', default="localhost", help='InfluxDb hostname or ip')
parser.add_argument('--dbport', default="8086", help='InfluxDb port number')
parser.add_argument('--dbname', help='InfluxDb database name', default='nmon')
parser.add_argument('--dbuser', help='InfluxDb user name', default='')
parser.add_argument('--dbpassword', help='InfluxDb user password', default='')
parser.add_argument('--batchsize', default=20000, help='How many inserts into InfluxDb in one chunk')
parser.add_argument('--verbose', default=0, help='Display all parameters used in the script')
parser.add_argument('--debug', default=0, help='Display processing and what is send to influx as line protocol')

args = parser.parse_args()

# Show arguments if verbose
if (args.verbose):    
    print("Filename=%s" %(args.filename))
    print("Batchsize=%d" %(args.batchsize))
    print("Dbhost=%s" %(args.dbhost))
    print("Dbport=%s" %(args.dbport))
    print("Dbuser=%s" %(args.dbuser))
    print("Dbname=%s" %(args.dbname))



for filename in glob.glob(args.filename):
    print("File: %s" % (filename))

    
    x = NMon.nmon(
        filename=filename, 
        dbhost=args.dbhost,
        dbport=args.dbport,
        dbuser=args.dbuser,
        dbpassword=args.dbpassword,
        dbname=args.dbname
        )

    print("Reading file", end='')
    x.readFile()
    print("done")

    
    # # process each CPU individually
    # for cpu in list(filter(lambda ak: 'CPU' in ak, x.showSections())):
    #     x.importSectionValues(measurement='cpu', tag='cpu=%s' % (cpu.lower()), section=cpu)
        
    # x.importSectionValues(measurement='mem', tag='', section='MEM')
    # x.importSectionValues(measurement='proc', tag='', section='PROC')
    # x.importSectionValues(measurement='vm', tag='', section='VM')
    # # x.importSection(measurement='jfs', tag='', section='JFSFILE')
    
    # x.importSectionTagValues(measurement='disk', tag='disk', section='DISKBUSY', valuename='disk_%busy')
    # x.importSectionTagValues(measurement='disk', tag='disk', section='DISKREAD', valuename='diskread_kb/s')
    # x.importSectionTagValues(measurement='disk', tag='disk', section='DISKWRITE', valuename='diskwrite_kb/s')
    # x.importSectionTagValues(measurement='disk', tag='disk', section='DISKXFER', valuename='diskxfer')
    # x.importSectionTagValues(measurement='disk', tag='disk', section='DISKBSIZE', valuename='diskbsize')

    # x.importSectionTagSplitValues(measurement='net', section='NET', splitvalue=-2)
    
    # x.importSectionTagSplitValues(measurement='netpacket', section='NETPACKET', splitvalue=-1)
    x.importTop(measurement='top', section='TOP')

    print(x.showSections())
    
    #x.importSection("CPU001")
    #x.importSection("CPU002")
    #x.importSection("JFSFILE")
    #x.importSection("NET")
    #x.importSection("NETPACKET")
    #x.importSection("TOP")
    
    x.writeDataPoints()