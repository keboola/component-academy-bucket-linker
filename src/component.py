'''
Template Component main class.

'''

import csv
import json
import logging
import os
import sys
import urllib

import requests
from kbc.env_handler import KBCEnvHandler
from kbcstorage.base import Endpoint

# configuration variables
KEY_SRC_TOKEN = '#src_token'
PAR_CONFIG_LISTS = 'configs.csv'
KEY_API_TOKEN = '#api_token'
KEY_REGION = 'aws_region'
KEY_DST_REGION = 'dst_aws_region'
# #### Keep for debug
KEY_DEBUG = 'debug'

MANDATORY_PARS = [KEY_API_TOKEN, KEY_REGION]
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.0.1'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS, log_level=logging.DEBUG if debug else logging.INFO)
        # override debug from config
        if self.cfg_params.get(KEY_DEBUG):
            debug = True
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config(MANDATORY_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)
            # default
        self.url_suffixes = {"US": ".keboola.com",
                             "EU": ".eu-central-1.keboola.com",
                             "AZURE-EU": ".north-europe.azure.keboola.com"}

        self.url_suffixes = {**self.url_suffixes, **self.image_params}

    def run(self):
        '''
        Main execution code
        '''
        params = self.cfg_params  # noqa
        src_project_id = os.getenv('KBC_PROJECTID')
        out_file_path = os.path.join(self.tables_out_path, 'user_projects_shared_buckets.csv')
        user_projects_buckets_path = os.path.join(self.tables_in_path, 'user_projects_buckets.csv')

        with open(user_projects_buckets_path, mode='rt', encoding='utf-8') as in_file, open(
                out_file_path, mode='w+',
                encoding='utf-8') as out_file:
            reader = csv.DictReader(in_file, lineterminator='\n')
            writer = csv.DictWriter(out_file, fieldnames=['project_id', 'dst_bucket_id', 'src_bucket_id'],
                                    lineterminator='\n')
            writer.writeheader()
            tokens = {}

            for row in reader:
                logging.info(f"Generating token for user {row['email']}, project {row['project_id']}")
                t = self.get_project_storage_token(params[KEY_API_TOKEN], row['project_id'], tokens,
                                                   region=params[KEY_DST_REGION])

                # #### LINK BUCKETS

                self.link_buckets(t['token'], src_project_id, row['project_id'], [row], writer,
                                  region=params[KEY_DST_REGION])

        self.configuration.write_table_manifest(out_file_path, primary_key=["project_id", "src_bucket_id"],
                                                incremental=True)
        logging.info('Finished!')

    def link_buckets(self, token, src_prj_id, project_id, buckets, log_writer, region='EU'):
        for b in buckets:
            try:
                lb = self.link_bucket(token, region, src_prj_id, b['bucket_id'], b['bucket_id'].split('in.c-')[1])
                log_writer.writerow({"dst_bucket_id": lb['id'],
                                     "project_id": project_id,
                                     "src_bucket_id": b['bucket_id']})
            except requests.HTTPError as e:
                logging.warning(
                    f'Linking to project {project_id} failed {json.loads(e.response.text)["error"]}')
                continue
            except Exception as e:
                logging.exception(e)

    def link_bucket(self, token, region, src_project_id, src_bucket_id, dst_name):
        """
        Link a bucket.

        Args:


        Returns:
            bucket (dict): Created bucket.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        cl = Endpoint('https://connection' + self.url_suffixes[region], 'buckets', token)
        url = cl.base_url
        parameters = {}
        parameters['name'] = dst_name
        parameters['sourceProjectId'] = src_project_id
        parameters['sourceBucketId'] = src_bucket_id

        header = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = urllib.parse.urlencode(parameters)
        return cl._post(url, data=data, headers=header)

    def generate_token(self, decription, manage_token, proj_id, region, expires_in=1800, manage_tokens=False,
                       additional_params=None):
        headers = {
            'Content-Type': 'application/json',
            'X-KBC-ManageApiToken': manage_token,
        }

        data = {
            "description": decription,
            "canManageBuckets": True,
            "canReadAllFileUploads": False,
            "canPurgeTrash": False,
            "canManageTokens": manage_tokens,
            "bucketPermissions": {"*": "write"},
            "expiresIn": expires_in
        }

        response = requests.post(
            f'https://connection{self.url_suffixes[region]}/manage/projects/' + str(proj_id) + '/tokens',
            headers=headers,
            data=json.dumps(data))
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            raise e
        else:
            return response.json()

    def get_project_storage_token(self, manage_token, project_id, storage_tokens, region='EU'):
        project_pk = f'{region}-{project_id}'
        if not storage_tokens.get(project_pk):
            logging.info(f'Generating token for project {region}-{project_id}')
            storage_tokens[project_pk] = self.generate_token('Sample Config provisioning', manage_token,
                                                             project_id, region, manage_tokens=True)
        return storage_tokens[project_pk]


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug_arg = sys.argv[1]
    else:
        debug_arg = False
    try:
        comp = Component(debug_arg)
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(1)
