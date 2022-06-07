from airflow.models import BaseOperator
import boto3
import os
from functools import reduce


class S3Operator(BaseOperator):
    # in the documentation it recommends doing static typing
    def __init__(self,
                 logger,
                 s3_credentials: dict,
                 data_path: str,
                 *args, **kwargs) -> None:
        self.logger = logger
        self.s3_credentials = s3_credentials
        self.data_path = data_path

        super().__init__(*args, **kwargs)

    def _save_file(self) -> None:
        self.logger.info("Insert files to S3")

        try:
            files = [file for file in os.listdir(self.data_path)
                     if os.path.isfile(self.data_path + file) and
                     file.endswith(".txt")]

            self.logger.info(files)

            for file in files:
                with open(self.data_path + file,
                          mode="r",
                          encoding="utf-8") as f_content:

                    # I thought about using the put_object function,
                    # although after seeing the difference with upload_file,
                    # I decided to use this one since it doesn't insert
                    # the whole body together in a single requests
                    self.s3_resource.Bucket(
                                         self.s3_credentials("_S3_BUCKETNAME")
                                        ).upload_file(self.data_path + file,
                                                      Key=file)

                    # close the file to prevent it from being corrupted
                    f_content.close()

        except Exception:
            self.logger.exception("Error inserting files to S3")
        finally:
            # print files loaded
            files_upload = reduce(lambda a, b: a + ", " + b,
                                  files)
            self.logger.info("Files uploaded: " + files_upload + ".")

    # Create s3 resource
    # after seeing the difference between session,
    # resource and client I preferred to use resource
    def _create_resource(self) -> None:
        try:
            # Create session
            s3_session = boto3.Session(
                aws_access_key_id=self.s3_credentials("_S3_PUBLICKEY"),
                aws_secret_access_key=self.s3_credentials("_S3_PRIVATEKEY")
            )
        except Exception:
            self.logger.exception("S3's session create error")

        # Create resource
        self.s3_resource = s3_session.resource("s3")

    def execute(self, context):
        self.logger.info("Saving .txt file to s3")
        self._create_resource()
        self._save_file()
