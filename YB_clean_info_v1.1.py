#!/usr/bin/python
# -*- coding:utf-8 -*-
__date__ = '20181212'
__author__ = 'cheng.chen@geneseeq.com'
import os
import sys
import subprocess
import re
import time
if len(sys.argv) < 2:
	print "python",sys.argv[0],"<FC>"
	exit(1)
FC = sys.argv[1]
cln_dir = "/GPFS01/GSPipeline"
out_dir = "/GPFS01/GSPipeline/YB_GSPipeline_CleanData_DCW/tmp/{}".format(FC)
clean_dir = "{}/clean_data".format(out_dir)
if not os.path.exists(clean_dir):
	os.makedirs(clean_dir)
tmp = os.popen("grep RT {}/20{}/id.list |cut -f 1 -d," .format (cln_dir,FC)).readlines()
lst = ''.join(list(tmp)).split("\n")[:-1]

def summary_treat(FC):
	'''
	extract the qc information of YB sample(s) from SUMMARY.csv
	'''
	head = "#SAMPLE,KIT,STAT,CLEAN_BASES(MB),MAPPING_RATE(%),200X_COVERAGE(%),200X_COVERAGE_DEDUP(%),CV_SCORE\n"
	out = open("{}/SUMMARY.csv" .format(out_dir),'w')
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
			stat += "Base warning"
		else:
			 stat += "|."
		if float(map_rate) < 95:
			stat += "|Mapping rate warning"
		else:
			stat += "|."
		if float(cov_200x) < 95:
			stat += "|200x coverage warning"
		else:
			stat += "|."
		if float(cv) > 1:
			stat += "|cv warning"
		else:
			stat += "|."
		if not "warning" in stat:
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
	cmd_out = open("{}/clean_data.sh".format(out_dir), 'w')
	for i in lst:
		r1 = "/GPFS01/GSPipeline/20{}/Results/{}/FASTQ/{}_R1.fastq.gz" .format(FC,i,i)
		r2 = "/GPFS01/GSPipeline/20{}/Results/{}/FASTQ/{}_R2.fastq.gz" .format(FC,i,i)
		o_r1 = os.path.basename(r1)
		o_r1 = "{}/{}" .format(clean_dir,o_r1)
		o_r2 = os.path.basename(r2)
		o_r2 = "{}/{}" .format(clean_dir,o_r2)
		cmd1 = "less {} | perl -alne ' my $line = $_;$line=~s/(^@)\\w+-\\w+\|..\|(.*)/\\1\\2/;print $line;\' | gzip > {}" .format(r1,o_r1)
		cmd2 = "less {} | perl -alne ' my $line = $_;$line=~s/(^@)\\w+-\\w+\|..\|(.*)/\\1\\2/;print $line;\' | gzip > {}" .format(r2,o_r2)
		cmd_out.write(cmd1+"\n")
		cmd_out.write(cmd2 + "\n")
def Job_monitor(job_list):
	'''
	monitor the job
	'''
	tmp = []
	while len(tmp) != len(job_list):
		p = subprocess.Popen(' bjobs -w|cut -f 1 -d " " ', stdout=subprocess.PIPE, shell=True)
		l = p.stdout.read()
		jobs = []
		for i in l.split('\n'):
			jobs.append(i.strip())
		tmp = [x for x in job_list if x not in jobs]
		time.sleep(60)
### basic treat ###
summary_treat(FC)
clean_treat(FC,out_dir)
### work ###
os.chdir(out_dir)
job_list = []
pattern = '<(\d+?)>'
pwd=os.getcwd()
p = subprocess.Popen("bb -c 7 clean_data.sh ", stdout=subprocess.PIPE, shell=True)
l = p.stdout.read()
for i in l.split('\n'):
	if re.search(pattern, i):
		job_list.append(re.search(pattern, i).group(1))
Job_monitor(job_list)
os.chdir(clean_dir)
os.system("md5sum *gz > md5sum.txt")














