import logging
import logging.config
import os

from google.cloud import storage
import cloudstorage as gcs
import webapp2
from google.appengine.api import app_identity
from pipelines.kfp.dependencies import BUCKET_LOCATION, LOGGING_CONF, PROJECT_ID
from pipelines.kfp.helpers.setup_credentials import setup_credentials

def create_client(
  project_id: str = PROJECT_ID,
):

    # If you don't specify credentials when constructing the client, the
    # client library will look for credentials in the environment.
    storage_client = storage.Client(project=project_id)

    # Make an authenticated API request 
    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = storage_class
    print(buckets)

setup_credentials()
logging.config.fileConfig(LOGGING_CONF)
logger = logging.getLogger("root")
    
def get_bucket(self):
  bucket_name = os.environ.get('BUCKET_NAME',
                               app_identity.get_default_gcs_bucket_name())

  self.response.headers['Content-Type'] = 'text/plain'
  self.response.write('Demo GCS Application running from Version: '
                      + os.environ['CURRENT_VERSION_ID'] + '\n')
  self.response.write('Using bucket name: ' + bucket_name + '\n\n')

def read_file(self, filename):
  self.response.write('Reading the full file contents:\n')

  gcs_file = gcs.open(filename)
  contents = gcs_file.read()
  gcs_file.close()
  self.response.write(contents)
  
def list_bucket(self, bucket):
  """Create several files and paginate through them.

  Production apps should set page_size to a practical value.

  Args:
    bucket: bucket.
  """
  self.response.write('Listbucket result:\n')

  page_size = 1
  stats = gcs.listbucket(bucket + '/foo', max_keys=page_size)
  while True:
    count = 0
    for stat in stats:
      count += 1
      self.response.write(repr(stat))
      self.response.write('\n')

    if count != page_size or count == 0:
      break
    stats = gcs.listbucket(bucket + '/foo', max_keys=page_size,
                           marker=stat.filename)
    
def delete_files(self):
  self.response.write('Deleting files...\n')
  for filename in self.tmp_filenames_to_clean_up:
    self.response.write('Deleting file %s\n' % filename)
    try:
      gcs.delete(filename)
    except gcs.NotFoundError:
      pass
