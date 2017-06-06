import sys, boto3, json, csv, time
from sys import argv
from pydoc import locate
from multiprocessing import pool

def splitFiles(fileName, threshold = 100000):
	with open(fileName) as largefile:
		header = '';
		linecount = -1
		filecount = 0
		fopen = 0
		splitnames = []
		for line in largefile:
			linecount += 1
			if linecount == 0:
				header = line
				continue
			if fopen == 0:
				splitname = 'split_' + str(filecount) + '.txt'
				splitnames.append(splitname)
				filecount += 1
				splitfile = open(splitname, 'w')
				fopen = 1
				splitfile.write(header)
			if linecount > 0:
				splitfile.write(line)
				
			if linecount % threshold == 0 and linecount > 0:
				splitfile.close()
				fopen = 0
		splitfile.close()
	return splitnames
					
					

def importToDynamoDB(tableName, csvFile, fields, config):
	
	# dynamo_resource = boto3.resource('dynamodb', region_name = config['aws_region'], aws_access_key_id = config['aws_access_key'], aws_secret_access_key = config['aws_secret_key'])
	# table = dynamo_resource.Table("LookupValues")
	with open(csvFile) as csv_file:
		csv_reader = csv.DictReader(csv_file, delimiter="\t")
		print("Start Writing data into table.", csvFile)
		# with table.batch_writer(overwrite_by_pkeys=[fields["Keys"]["partition"], fields["Keys"]["sort"]]) as batch:
		# 	for row in csv_reader:
		# 		for key, val in row.items():
		# 			type = locate(fields[key]);
		# 			row[key] = str('$E$') if (not val or val=='') else type(val)
		# 		batch.put_item(
		# 			Item=row
		# 		)
		# print("End Writing data into table.")

def truncateTable(tableName, fields):
	#Todo
	print('This is in progress')
	
def setCapacity(tableName, config):
	client = boto3.client('dynamodb', region_name = config['aws_region'], aws_access_key_id = config['aws_access_key'], aws_secret_access_key = config['aws_secret_key'])

	table = client.describe_table(
			TableName=tableName
		)
		
	#If through put are already same, do not request
	if(table["Table"]["ProvisionedThroughput"]["ReadCapacityUnits"] != int(config['cap'][0]) or table["Table"]["ProvisionedThroughput"]["WriteCapacityUnits"] != int(config['cap'][1])):
		table = client.update_table(
			TableName = tableName,
			ProvisionedThroughput = {
				'ReadCapacityUnits': int(config['cap'][0]),
				'WriteCapacityUnits': int(config['cap'][1])
			}
		)

	attempt = 0
	#Wait until throughput is increased
	while table["Table"]["TableStatus"] != 'ACTIVE':
		time.sleep(5)
		table = client.describe_table(
			TableName=tableName
		)
		attempt += 1
		if attempt > 5:
			print("Warning!! Could not set the capacity of the table")
			break

	print("Capacity update completed")
	return table
#End of SetCapacity

def getopts(argv):
	opts = {}
	while argv:
		if argv[0][0] == '-' and argv[1]:
			opts[argv[0]] = argv[1];
		argv = argv[1:]
	return opts

def main():

	tableName = ""
	csvFile = ""
	err = ""
	fields = {}
	config = {}
	read_cap = 1
	write_cap = 200
	
	myargs = getopts(argv)
	if '-i' in myargs:
		csvFile = myargs['-i']
	else:
		err += "Error!! No input file name is provided. Use -i [FILENAME]\n"
	if '-t' in myargs:
		tableName = myargs['-t']
	else:
		err += "Error!! No DynamoDB table name is provided. Use -t [TABLE NAME]\n"

	if '-ak' in myargs:
		config['aws_access_key'] = myargs['-ak']
	else:
		config['aws_access_key'] = 'AWS_ACCESS_KEY'
		
	if '-sk' in myargs:
		config['aws_secret_key'] = myargs['-sk']
	else:
		config['aws_secret_key'] = 'AWS_SECRET_KEY'
		
	if '-region' in myargs:
		config['aws_region'] = myargs['-region']
	else:
		config['aws_region'] = 'us-east-1'

	if '-c' in myargs:
		config['cap'] = myargs['-c'].split(',')
	else:
		config['cap'] = [read_cap, write_cap]
	
	with open('tablenames.json') as tablenameFile:
		tablenames = json.load(tablenameFile)
	
	#Load table names
	try:
		fields = tablenames[tableName]
	except KeyError:
		err += "Error!! Table fields are not configured in tablenames.json"
		pass
	
	if err != '':
		print(err)
		sys.exit(0)

	if '-c' in myargs:
		setCapacity(tableName, config)
	filenames = splitFiles(csvFile)

	print(filenames)
	filecount = len(filenames)
	pool = Pool(filecount)
	pool.map()
	#importToDynamoDB(tableName, csvFile, fields, config)


if __name__ == "__main__":
	start = time.time()
	print('Started at: ' + time.ctime(start))
	time.sleep(3)
	main()
	end = time.time()
	print('Ends at: ' + time.ctime())
	print('Run time:' + str(round(end-start)) + ' sec.')
	#cProfile.run('main()') # if you want to do some profilings