import os
gcloud_sdk_path = '/home/tariqanwarph/Downloads/google-cloud-sdk/bin/'
filename_job = os.path.abspath(os.path.join(os.path.dirname(__file__),"..","..","job.txt"))

    # Write job name to file
with open(filename_job, 'r') as infile:
    job_name = infile.read()

gc_command = f'{gcloud_sdk_path}gcloud dataflow jobs cancel '\
f'{job_name} ' \
'--region us-central1 '
os.system(gc_command)
