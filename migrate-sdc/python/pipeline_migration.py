from streamsets.sdk import DataCollector
from streamsets.sdk import ControlHub

#Connect to Standalone SDC
sdc = DataCollector('http://<sdc-hostname>:18630')

#Connect to Control Hub
control_hub = ControlHub('https://cloud.streamsets.com',username='<user>@<org>',password='<password>')

#Migrate all pipelines from SDC to Control Hub

#Export All Pipelines from SDC
sdc_pipelines = sdc.export_pipelines(sdc.pipelines,
include_library_definitions=True,
include_plain_text_credentials=True)

# Write to an archive the exported pipeline
with open('/tmp/pipelines.zip', 'wb') as pipelines_zip_file:
   pipelines_zip_file.write(sdc_pipelines)

# Import the pipeline from the above archive
with open('/tmp/pipelines.zip', 'rb') as input_file:
   pipelines_imported = control_hub.import_pipelines_from_archive(input_file, 'Import Pipeline using SDK')

#Create Jobs from Pipelines
job_builder = control_hub.get_job_builder()
pipelines = control_hub.pipelines
#Example of pulling just one specific pipeline
#pipelines = control_hub.pipelines.get_all(name='Dev to Trash')
for pipeline in pipelines:
    job = job_builder.build(pipeline.name, pipeline=pipeline)
    control_hub.add_job(job)

#Stop All Pipelines in SDC
sdc_pipelines=sdc.pipelines
for sdc_pipeline in sdc_pipelines:
    sdc.stop_pipeline(sdc_pipeline)

#Get Job
job = control_hub.jobs.get(job_name='Dev to Trash')

#Upload offset
with open('/var/lib/sdc/runInfo/DevtoTrac258a7d8-d915-42e2-8aa7-890bc45e4f73/0/offset.json') as offset_file:
    control_hub.upload_offset(job, offset_file=offset_file)
# PermissionError: [Errno 13] Permission denied - check the Linux permissions

#Update job with proper SDC label
job.data_collector_labels = ['dev']
control_hub.update_job(job)

#Start job
control_hub.start_job(job)


