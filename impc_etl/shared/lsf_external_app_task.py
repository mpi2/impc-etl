from luigi.contrib.lsf import LSFJobTask
import os
import subprocess
import logging
import shutil
import random
from luigi.task_status import PENDING, FAILED, DONE, RUNNING, UNKNOWN
import time

LOGGER = logging.getLogger("luigi-interface")


def track_job(job_id):
    """
    Tracking is done by requesting each job and then searching for whether the job
    has one of the following states:
    - "RUN",
    - "PEND",
    - "SSUSP",
    - "EXIT"
    based on the LSF documentation
    """
    cmd = "bjobs -noheader -o stat {}".format(job_id)
    track_job_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    status = track_job_proc.communicate()[0].decode("utf-8").strip("\n")
    return status


class LSFExternalJobTask(LSFJobTask):
    app = ""
    input_args = []

    def _run_job(self):
        """
        Build a bsub argument that will run lsf_runner.py on the directory we've specified.
        """

        args = []

        if isinstance(self.output(), list):
            log_output = os.path.split(self.output()[0].path)
        else:
            log_output = os.path.split(self.output().path)

        args += ["bsub"]
        if self.queue_flag != "queue_name":
            args += ["-q", self.queue_flag]
        args += ["-n", str(self.n_cpu_flag)]
        args += ["-M", str(self.memory_flag)]
        args += ["-R", "rusage[%s]" % self.resource_flag]
        args += ["-W", str(self.runtime_flag)]
        if self.job_name_flag:
            args += ["-J", str(self.job_name_flag)]
        args += ["-o", os.path.join(log_output[0], "job.out")]
        args += ["-e", os.path.join(log_output[0], "job.err")]
        if self.extra_bsub_args:
            args += self.extra_bsub_args.split()

        args += [self.app]
        args += self.app_options()

        # That should do it. Let the world know what we're doing.
        LOGGER.info("### LSF SUBMISSION ARGS: %s", " ".join([str(a) for a in args]))

        # Submit the job
        run_job_proc = subprocess.Popen(
            [str(a) for a in args],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            cwd=self.tmp_dir,
        )
        output = run_job_proc.communicate()[0]

        # ASSUMPTION
        # The result will be of the format
        # Job <123> is submitted ot queue <myqueue>
        # So get the number in those first brackets.
        # I cannot think of a better workaround that leaves logic on the Task side of things.
        LOGGER.info("### JOB SUBMISSION OUTPUT: %s", str(output))
        self.job_id = int(str(output).split("<")[1].split(">")[0])
        LOGGER.info(
            "Job %ssubmitted as job %s", self.job_name_flag + " ", str(self.job_id)
        )

        self._track_job()

        # If we want to save the job temporaries, then do so
        # We'll move them to be next to the job output
        if self.save_job_info:
            LOGGER.info("Saving up temporary bits")

            # dest_dir = self.output().path
            shutil.move(self.tmp_dir, "/".join(log_output[0:-1]))

        # Now delete the temporaries, if they're there.
        self._finish()

    def app_options(self):
        pass

    def _init_local(self):

        base_tmp_dir = self.shared_tmp_dir

        random_id = "%016x" % random.getrandbits(64)
        task_name = random_id + self.task_id
        # If any parameters are directories, if we don't
        # replace the separators on *nix, it'll create a weird nested directory
        task_name = task_name.replace("/", "::")

        # Max filename length
        max_filename_length = os.fstatvfs(0).f_namemax
        self.tmp_dir = os.path.join(base_tmp_dir, task_name[:max_filename_length])

        LOGGER.info("Tmp dir: %s", self.tmp_dir)
        os.makedirs(self.tmp_dir)

        # Now, pass onto the class's specified init_local() method.
        self.init_local()

    def _track_job(self):
        time0 = 0
        while True:
            # Sleep for a little bit
            time.sleep(self.poll_time)

            # See what the job's up to
            # ASSUMPTION
            lsf_status = track_job(self.job_id)
            if lsf_status == "RUN":
                self.job_status = RUNNING
                LOGGER.info("Job is running...")
                if time0 == 0:
                    time0 = int(round(time.time()))
            elif lsf_status == "PEND":
                self.job_status = PENDING
                LOGGER.info("Job is pending...")
            elif lsf_status == "DONE" or lsf_status == "EXIT":
                # Then the job could either be failed or done.
                errors = self.fetch_task_failures()
                if not errors:
                    self.job_status = DONE
                    LOGGER.info("Job is done")
                    time1 = int(round(time.time()))

                    # Return a near estimate of the run time to with +/- the
                    # self.poll_time
                    job_name = str(self.job_id)
                    if self.job_name_flag:
                        job_name = "%s %s" % (self.job_name_flag, job_name)
                    LOGGER.info(
                        "### JOB COMPLETED: %s in %s seconds",
                        job_name,
                        str(time1 - time0),
                    )
                else:
                    self.job_status = FAILED
                    LOGGER.error("Job has FAILED")
                    LOGGER.error("\n\n")
                    LOGGER.error("Traceback: ")
                    for error in errors:
                        LOGGER.error(error)
                break
            elif lsf_status == "SSUSP":
                self.job_status = PENDING
                LOGGER.info("Job is suspended (basically, pending)...")

            else:
                self.job_status = UNKNOWN
                LOGGER.info("Job status is UNKNOWN!")
                LOGGER.info("Status is : %s", lsf_status)
                break
