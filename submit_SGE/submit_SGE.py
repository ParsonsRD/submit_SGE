"""
The purpose of this code is to allow the submission of a large number of shell commands
to the sun grid engine (SGE). This also allows a maximum number of jobs to be submitted
and wait until the job number falls below this number.
"""

import os
import getpass
import time
import subprocess
__all__ = ['SubmitSGE']


class SubmitSGE:

    def __init__(self, queue_name="", extra_options="", maximum_jobs=100, queue_update=60,
                 verbose=True):
        """
        Class for submitting large number of jobs to the sun grid engine

        :param queue_name: str
            Name of SGE queue to submit to (uses default if empty)
        :param extra_options: str
            Extra option to add to qsub command
        :param maximum_jobs: int
            Maximum number of jobs to submit
        :param queue_update: float
            Time to wait between refreshes of the queue content
        :param verbose: bool
            Set verbosity level
        """
        self.queue_name = queue_name
        self.verbose = verbose
        self.extra_options = extra_options
        self.queue_update = queue_update
        self.maximum_jobs = maximum_jobs

    def get_jobs_in_queue(self):
        """
        Get the total number of jobs in the SGE queue

        :return: int
            Number of total jobs in queue
        """
        username = getpass.getuser()

        # All we want to do here is call qstat and count the remaining lines
        stat = "qstat -u " + username
        if self.queue_name is not "":
            stat += "-q " + self.queue_name

        # Call qstat command
        f = os.popen(stat)

        job_number = len(f.readlines())

        # If not empty remove the first 2 lines of table
        if job_number > 0:
            job_number -= 2
        return job_number

    def get_jobs_in_queue_name(self, name_fragment):
        """
        Get the number of jobs in the SGE queue containing a name_fragment

        :param name_fragment: str
            Common name element tto search for
        :return: int
            Number of jobs containing fragment
        """
        username = getpass.getuser()

        # As before call qstat and count lines
        # This code contains a horrible regex, but essentiall allows me to get the full
        # job name from qstat
        stat = "qstat -u " + username + " -xml "
        if self.queue_name is not "":
            stat += "-q " + self.queue_name

        f = os.popen(stat)

        # Search through lines for our name fragment
        job_number = 0
        for i in f.readlines():
            if i.find(name_fragment) > -1:
                job_number = job_number + 1

        if self.verbose:
            print(job_number, "Jobs containing string", name_fragment, "in queue")

        return job_number

    def submit_job(self, command, job_name):
        """
        Submit a shell command to the SGE queue

        :param command: str
            Command to submit
        :param job_name: str
            Name of job
        :return: None
        """
        # Create shell script to submit to SGE
        filename = "./submit_" + job_name + ".sh"

        # First copy our useful environment variables
        libpath = os.environ.get("LD_LIBRARY_PATH")
        path = os.environ.get("PATH")
        pypath = os.environ.get("PYTHONPATH")
        # Get conda evironment if we have one
        conda_environment = os.environ.get("CONDA_PREFIX")

        f = open(filename, 'w')
        f.write("#/bin/sh \n \n")
        f.write("export LD_LIBRARY_PATH=" + libpath + " \n")
        f.write("export PATH=" + path + " \n")
        f.write("export PYTHONPATH=" + pypath + " \n")
        # Load conda if we want to
        if conda_environment is not "":
            environment_name = conda_environment.split("/")[-1]
            f.write("source activate" + environment_name + " \n")

        f.write('echo -------------------------------------\n')
        f.write('echo "USER    : $USER" \n')
        f.write('echo "JOB_ID  : $JOB_ID" \n')
        f.write('echo "JOB_NAME: $JOB_NAME" \n')
        f.write('echo "HOSTNAME: $HOSTNAME" \n')
        f.write('echo -------------------------------------\n')
        f.write("ulimit -c 1\n")  # prevent big core dumps!

        f.write(command)

        f.close()

        # Create qsub call and use it
        call_command = ["qsub", self.extra_options, "-notify", "-N", job_name, filename]
        subprocess.call(call_command)

        time.sleep(1)

        os.remove(filename)

    def submit_job_when_ready(self, command, job_name):
        """
        Submit a job to the SGE queue when the number of submitted jobs is below the
        maximum allowed

        :param command: str
            Command to submit
        :param job_name: str
            Name of job
        :return: None
        """
        job_number = self.get_jobs_in_queue()
        if self.verbose:
            print(job_number, "out of", self.maximum_jobs,
                  "allowed jobs currently submitted")

        while True:
            job_number = self.get_jobs_in_queue()

            if job_number < self.maximum_jobs:
                if self.verbose:
                    print("Submitting Job", job_name)
                self.submit_job(command, job_name)
                break
            else:
                time.sleep(self.queue_update)

    def submit_job_list(self, command_list, job_name, wait_for_completion=True):
        """
        Submit a list of commands to the cluster and wait for their completion if needed

        :param command_list: list
            List of commands to submit
        :param job_name: str
            Name of job
        :param wait_for_completion: bool
            Should we wait for job completion
        :return:
        """
        for command in command_list:
            self.submit_job_when_ready(command, job_name)

        if wait_for_completion:
            while self.get_jobs_in_queue_name(job_name) > 0:
                time.sleep(self.queue_update)
