
General : An Apache Beam prgram to join two dataset files patient events & users events.

pyhoon version: 3.8

Python packages to install: pip install -r /path/to/requirements.txt (scr/requirements.txt)

run example: python main.py --input1 events.txt --input2 users.txt --output out.txt

input : two json rowed files.

output : one json rowed file.

pip install 'apache-beam[gcp]'

run on GCP DATA FLOW :

pip install 'apache-beam[gcp]'

python src/main.py .py \
  --project=$PROJECT --region= \
  --runner=DataflowRunner \
  --staging_location=gs://$PROJECT/.... \
  --temp_location gs://$PROJECT/tmp \
  --input1 gs://$PROJECT/data_files/events.txt \
  --input2 gs://$PROJECT/data_files/users.txt \
  --output gs://$PROJECT/data_files/out.txt\
  --save_main_session