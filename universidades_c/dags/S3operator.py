from logging import exception
from pyclbr import Function
from typing import Dict, List, Optional

from airflow.models import BaseOperator
import boto3


class S3Operator(BaseOperator):
    """
    Creates a Session Object connecting with S3 and uploads files to it.`

    :param loggerfunc: A function object to configurate de logger.
    :type Function: function()
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (should contain the S# credentials, paths to files and
        names for the files to be uploaded)
    :type op_kwargs: dict (templated)
    :param templates_dict: a dictionary where the values are templates that
    """

    template_fields = ('templates_dict', 'op_args', 'op_kwargs')
    template_fields_renderers = {"templates_dict": "json", "op_args": "py",
                                 "op_kwargs": "py"}
    BLUE = '#ffefeb'
    ui_color = BLUE

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects(e.g protobuf).
    shallow_copy_attrs = (
        'loggerfunc',
        'op_kwargs',
    )

    def __init__(
        self,
        *,
        names: Optional[Dict] = None,
        loggerfunc: Optional[Function] = None,
        op_args: Optional[List] = None,
        op_kwargs: Optional[Dict] = None,
        templates_dict: Optional[Dict] = None,
        templates_exts: Optional[List[str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.names = names
        self.loggerfunc = loggerfunc
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.templates_dict = templates_dict
        if templates_exts:
            self.template_ext = templates_exts

    def execute(self, context: Dict):
        return_value = self.execute_callable()
        return return_value

    def s3_method(
        self,
        names: dict,
        bucket: str,
        p_key: str,
        s_key: str,
    ):
        """
        Executes the files uploading to S3 by looping through the names
        dictionary given, using the S3 credentials injected through kwargs.
        """

        loggr = self.loggerfunc()
        loggr.info('Starting Upload to S3 process.')
        # Upload the file
        try:
            session = boto3.Session(
                aws_access_key_id=p_key,
                aws_secret_access_key=s_key
            )
            s3 = session.resource('s3')
            loggr.info('    S3 session created')
        except Exception:
            loggr.info('S3 session could not be created, retrying.')
            return False
        # Creating S3 Resource From the Session.
        for name, path in names.items():
            try:
                s3.Bucket(bucket).upload_file(
                                path,
                                name)
                loggr.info(f'   File {name} uploaded to S3')
            except Exception:
                loggr.info(f'File {name} could not be uploaded.')
                return False
        loggr.info(f'ETL process for universitiy {name} completed!!!\n')
        return True

    def execute_callable(self):
        """
        Calls the upload data method with the given arguments.

        :return: the return value of the call.
        :rtype: any
        """

        return self.s3_method(self.names, **self.op_kwargs)
