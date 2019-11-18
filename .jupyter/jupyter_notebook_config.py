from s3contents import S3ContentsManager
import os

c = get_config()

# Add testing password
c.NotebookApp.password = 'sha1:492609a8c50e:050208db9dd1a56ae8bc4524527e3a0f896743e0'

# Tell Jupyter to use S3ContentsManager for all storage.
c.NotebookApp.contents_manager_class = S3ContentsManager
c.S3ContentsManager.access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
c.S3ContentsManager.secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
c.S3ContentsManager.bucket = os.getenv('ANALYTICS_S3_BUCKET', "dwh-analytics")
# c.S3ContentsManager.prefix = "notebooks"
