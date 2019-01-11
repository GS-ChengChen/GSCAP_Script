#!/usr/bin/python
# -*- coding:utf-8 -*-
__date__ = '20181211'
__author__ = 'cheng.chen@geneseeq.com'
import os
import sys
import subprocess
if len(sys.argv) < 3:
	print "python",sys.argv[0],"<FC> <out_dir>"
	exit(1)
FC = sys.argv[1]
out_dir = sys.argv[2]

cln_dir = "/GPFS01/GSPipeline"
yb_dir = "/GPFS01/GSPipeline/YB_GSPipeline_CleanData_DCW"
# out_dir = "{}/{}/clean_data" .format (yb_dir,FC)
# clean_dir = "%s/clean_data" % out_dir
## make YB data folder
 # if not os.path.exists(out_dir):
 # 	os.makedirs(out_dir)
if not os.path.exists(out_dir):
	os.makedirs("{}/{}/clean_data".format(out_dir,FC))
tmp = os.popen("grep ^YB {}/20{}/id.list |cut -f 1 -d," .format (cln_dir,FC)).readlines()
lst = ''.join(list(tmp)).split("\n")[:-1]

def summary_treat(FC):
	'''
	extract the qc information of YB sample(s) from SUMMARY.csv
	'''
	head = "#SAMPLE,KIT,STAT,CLEAN_BASES(MB),MAPPING_RATE(%),200X_COVERAGE(%),200X_COVERAGE_DEDUP(%,CV_SCORE\n"
	out = open("SUMMARY.csv",'w')
	out.write(head)
	summarry = "/GPFS01/GSPipeline/20{}/SUMMARY.csv".format(FC)
	f = open(summarry,'r')
	f.readline() ## skip the head line
	dic = {}
	for i in f:
		i = i.strip().split(",")
		id,kit,clean_base,map_rate,cov_200x,cov_200x_dup,cv= i[0],i[3],i[15],i[25],i[32],i[44],i[50]
		stat = ""
		if float(clean_base)/1000 < 5:
			stat = "BASE FAILED"
		elif float(map_rate) < 95:
			stat = "MAPPING RATE FAILED"
		elif float(cov_200x) < 95:
			stat = "COVERAGE FAILED"
		elif float(cv) > 1:
			stat = "CV FAILED"
		else:
			stat = "PASS"
		k = id
		v = ",".join([id,kit,stat,clean_base,map_rate,cov_200x,cov_200x_dup,cv])
		dic[k] = v
	f.close()
	for each in lst:
		out.write(dic[each]+"\n")
	out.close()

def clean_treat(FC,out_dir):
	'''
	generate the clean data of standard fastq format
	'''
	for i in lst:
		r1 = "/GPFS01/GSPipeline/20{}/Results/{}/FASTQ/{}_R1.fastq.gz" .format(FC,i,i)
		r2 = "/GPFS01/GSPipeline/20{}/Results/{}/FASTQ/{}_R2.fastq.gz" .format(FC,i,i)
		o_r1 = os.path.basename(r1)
		o_r1 = "{}/{}" .format(out_dir,o_r1)
		o_r2 = os.path.basename(r2)
		o_r2 = "{}/{}" .format(out_dir,o_r2)
		cmd1 = "zless {} | perl -alne ' my $line = $_;$line=~s/(^@)\\w+-\\w+\|..\|(.*)/\\1\\2/;print $line;\' | gzip > {}" .format(r1,o_r1)
		cmd2 = "zless {} | perl -alne ' my $line = $_;$line=~s/(^@)\\w+-\\w+\|..\|(.*)/\\1\\2/;print $line;\' | gzip > {}" .format(r2,o_r2)
		cmd_out = open("{}/clean_data.sh".format(out_dir),'w')
		cmd_out.write(cmd1+"\n")
		cmd_out.write(cmd2 + "\n")
		# print cmd1
		# print cmd2
def Job_monitor(job_list):
	'''
	monitor the job
	'''
	tmp = []
	while len(tmp) != len(job_list):
		tmp = os.popen(' bjobs -w|cut -f 1 -d " " ').readlines
		jobs = ''.join(list(tmp)).split("\n")[:-1]
		tmp = [x for x in job_list if x not in jobs]
		time.sleep(60)
os.chdir(out_dir)
job_list = []
pattern = '<(\d+?)>'
p = subprocess.Popen("bsubjobs.py clean_data.sh -c 7 ", stdout=PIPE, shell=True)
l = p.stdout.read()
for i in l.split('\n'):
	if re.search(pattern, i):
		job_list.append(re.search(pattern, i).group(1))

summary_treat(FC)
clean_treat(FC,out_dir)
Job_monitor(job_list)
os.chdir("{}/{}/clean_data".format(out_dir,FC))
os.system("md5sum *gz > md5sum.txt")














