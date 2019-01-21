#!/usr/bin/python
# -*- coding:utf-8 -*-
__date__ = '20181219'
__author__ = 'cheng.chen@geneseeq.com'
import os
import sys
import subprocess
import re
import time
import gzip
if len(sys.argv) < 2:
	print "python",sys.argv[0],"<FC>"
	print "Example:\nnohup python",sys.argv[0],"181207_K00141_0286_AHYC7MBBXX &"
	exit(1)
FC = sys.argv[1]
cln_dir = "/GPFS01/GSPipeline"
out_dir = "/GPFS01/GSPipeline/YB_GSPipeline_CleanData_DCW/{}".format(FC)
clean_dir = "{}/clean_data".format(out_dir)
cnv_dir = "{}/CNV" .format(out_dir)
#if not os.path.exists(clean_dir):
#	os.makedirs(clean_dir)
#if not os.path.exists(cnv_dir):
#	os.makedirs(cnv_dir)
tmp = os.popen("grep ^YB {}/20{}/id.list |cut -f 1 -d," .format (cln_dir,FC)).readlines()
lst = ''.join(list(tmp)).split("\n")[:-1]
if len(lst) >= 1:
	if not os.path.exists(clean_dir):
		os.makedirs(clean_dir)
	if not os.path.exists(cnv_dir):
		os.makedirs(cnv_dir)
def summary_treat(FC):
	'''
	extract the qc information of YB sample(s) from SUMMARY.csv
	'''
	head = "#SAMPLE,KIT,LANE,STAT,CLEAN_BASES(MB),MAPPING_RATE(%),200X_COVERAGE(%),200X_COVERAGE_DEDUP(%),CV_SCORE\n"
	out = open("{}/SUMMARY.csv" .format(out_dir),'w')
	out.write(head)
	summarry = "/GPFS01/GSPipeline/20{}/SUMMARY.csv".format(FC)
	f = open(summarry,'r')
	f.readline() ## skip the head line
	dic = {}
	for line in f:
		i = line.strip().split(",")
		id,kit,lane,clean_base,map_rate,cov_200x,cov_200x_dup,cv= i[0],i[3],i[4],i[15],i[25],i[32],i[44],i[50]
		stat = []
		stat.append(id)
		if float(clean_base)/1000 < 5:
			stat.append("Base warning")
		if float(map_rate) < 95:
			stat.append("Mapping rate warning")
		if float(cov_200x) < 95:
			stat.append("200X coverage warning")
		if float(cv) > 1:
			stat.append("cv warning")
		if float(clean_base)/1000 >= 5 and float(map_rate) >= 95 and float(cov_200x) >= 95 and float(cv) <= 1:
			stat = ["PASS"]
		stat = ";".join(list(stat))
		k = id
		v = ",".join([id,kit,lane,stat,clean_base,map_rate,cov_200x,cov_200x_dup,cv])
		dic[k] = v
	f.close()
	for each in lst:
		out.write(dic[each]+"\n")
	out.close()
def cnv_treat(FC,cnv_dir):
	'''
	copy the cnv result from the analysis folder
	'''
	for i in lst:
		id_dir = "{}/{}".format(cnv_dir,i)
		if not os.path.exists(id_dir):
			os.makedirs(id_dir)
		os.system("cp -pr /GPFS01/GSPipeline/20{}/Results/{}/CNV/* {}".format(FC,i,id_dir))
		os.system("rm -rf {}/temp" .format(id_dir))
def clean_treat(FC,out_dir):
	'''
	generate the clean data of standard fastq format
	'''
	cmd_out = open("{}/clean_data.sh".format(out_dir), 'w')
	for i in lst:
		r1 = "/GPFS01/GSPipeline/20{}/Results/{}/FASTQ/{}_R1.fastq.gz" .format(FC,i,i)
		r2 = "/GPFS01/GSPipeline/20{}/Results/{}/FASTQ/{}_R2.fastq.gz" .format(FC,i,i)
		head = gzip.open(r1).readline().strip()
		o_r1 = os.path.basename(r1)
		o_r1 = "{}/{}" .format(clean_dir,o_r1)
		o_r2 = os.path.basename(r2)
		o_r2 = "{}/{}" .format(clean_dir,o_r2)
		if "|" in head:
			cmd1 = "less {} | perl -alne ' my $line = $_;$line=~s/(^@)\\w+-\\w+\|..\|(.*)/\\1\\2/;print $line;\' | gzip > {}" .format(r1,o_r1)
			cmd2 = "less {} | perl -alne ' my $line = $_;$line=~s/(^@)\\w+-\\w+\|..\|(.*)/\\1\\2/;print $line;\' | gzip > {}" .format(r2,o_r2)
		if "|" not in head and head.count(":") == 9:
			cmd1 = "ln -s {} {}" .format(r1,o_r1)
			cmd2 = "ln -s {} {}".format(r2, o_r2)
		if head.count(":") > 9:
			cmd1 = "less {} | perl -alne ' my $line = $_;$line=~s/(^@)\\w+-\\w+:..:(.*)/\\1\\2/;print $line;\' | gzip > {}" .format(r1,o_r1)
			cmd2 = "less {} | perl -alne ' my $line = $_;$line=~s/(^@)\\w+-\\w+:..:(.*)/\\1\\2/;print $line;\' | gzip > {}" .format(r2,o_r2)
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
cnv_treat(FC,cnv_dir)
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














